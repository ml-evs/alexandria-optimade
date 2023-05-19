import bz2
import json
from pathlib import Path
from typing import List, Optional
import datetime

import pymongo
import tqdm
from optimade.adapters import Structure
from optimade.models.references import ReferenceResource
from optimade.server.routers import references, structures

ARCHIVE_TIMESTAMP = "2023-05-01T07:57:59.147136+00:00"


def ingest_and_insert_pymatgen_bz2(
    data_path: Path, attach_references: Optional[List[ReferenceResource]] = None
):
    from pymatgen.entries.computed_entries import ComputedStructureEntry

    structures_coll = structures.structures_coll.collection
    try:
        structures_coll.create_index("id", unique=True)
    except pymongo.errors.OperationFailure:
        print("Dropping existing malformed collection")
        structures_coll.drop()

    if attach_references:
        references_coll = references.references_coll.collection
        try:
            references_coll.create_index("id", unique=True)

        except pymongo.errors.OperationFailure:
            print("Dropping existing malformed collection")
            references_coll.drop()

        for ref in attach_references:
            try:
                references_coll.insert_one(ref.dict(exclude_unset=True))
            except pymongo.errors.DuplicateKeyError:
                pass

    with bz2.open(data_path) as fh:
        data = json.loads(fh.read().decode("utf-8"))

    for ind, entry in tqdm.tqdm(enumerate(data["entries"]), total=len(data["entries"])):
        computed_entry = ComputedStructureEntry.from_dict(entry)
        optimade_doc = Structure.ingest_from(computed_entry.structure)
        optimade_doc.entry.id = computed_entry.data["mat_id"]
        optimade_doc.entry.attributes.last_modified = datetime.datetime.fromisoformat(
            ARCHIVE_TIMESTAMP
        )
        optimade_doc.entry.attributes.immutable_id = optimade_doc.entry.id
        if attach_references:
            optimade_doc.entry.relationships = {
                "references": [
                    {"type": "references", "id": ref.id} for ref in attach_references
                ]
            }

        database_entry = optimade_doc.entry.dict(exclude_unset=True)
        database_entry["attributes"]["energy"] = computed_entry.energy
        database_entry["attributes"]["xc_functional"] = "SCAN"
        database_entry["attributes"]["hull_distance"] = computed_entry.data[
            "e_above_hull"
        ]
        database_entry["attributes"]["formation_energy_per_atom"] = computed_entry.data[
            "e_form"
        ]
        database_entry["attributes"]["band_gap"] = min(
            computed_entry.data["band_gap_ind"], computed_entry.data["band_gap_dir"]
        )
        database_entry["attributes"]["band_gap_direct"] = computed_entry.data[
            "band_gap_dir"
        ]
        database_entry["attributes"]["band_gap_indirect"] = computed_entry.data[
            "band_gap_ind"
        ]
        database_entry["attributes"]["prototype_id"] = computed_entry.data[
            "prototype_id"
        ]
        database_entry.update(database_entry.pop("attributes"))
        try:
            structures_coll.update_one(
                {"id": optimade_doc.entry.id}, {"$set": database_entry}, upsert=True
            )
        except pymongo.errors.DuplicateKeyError:
            pass

    print(f"Successfully ingested {ind+1} structures.")


if __name__ == "__main__":
    ref = ReferenceResource(
        id="10.24435/materialscloud:5j-9m",
        type="references",
        attributes={
            "doi": "https://doi.org/10.24435/materialscloud:5j-9m",
            "last_modified": datetime.datetime.fromisoformat(ARCHIVE_TIMESTAMP),
        },
    )
    for archive_path in tqdm.tqdm(
        (Path(__file__).parent.parent / "data").glob("alexandria_scan_*.json.bz2")
    ):
        ingest_and_insert_pymatgen_bz2(
            archive_path,
            attach_references=[ref],
        )

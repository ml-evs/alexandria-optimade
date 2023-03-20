import os
from pathlib import Path

os.environ["OPTIMADE_CONFIG_FILE"] = str(Path(__file__).parent.parent / "optimade_config.json")

from optimade.adapters import Structure
from optimade.server.routers import structures

def ingest_and_insert_pymatgen_bz2(data_path: Path):
    from pymatgen.entries.computed_entries import ComputedStructureEntry

    
    structures_coll = structures.structures_coll

    import json, bz2

    with bz2.open(data_path) as fh:
      data = json.loads(fh.read().decode('utf-8'))

    for ind, entry in enumerate(data["entries"]):
        computed_entry = ComputedStructureEntry.from_dict(entry)
        optimade_doc = Structure.ingest_from(computed_entry.structure)
        optimade_doc.entry.id = str(ind)
        optimade_doc.entry.attributes.immutable_id = str(ind)
        database_entry = optimade_doc.entry.dict(exclude_unset=True)
        database_entry["attributes"]["energy"] = computed_entry.energy
        database_entry["attributes"]["xc_functional"] = "PBESol"

        structures_coll.insert([database_entry])





if __name__ == "__main__":
    ingest_and_insert_pymatgen_bz2(Path(__file__).parent.parent / "data" / "2021.04.06_ps.json.bz2")
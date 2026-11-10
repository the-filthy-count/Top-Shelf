"""Reserve separate folders for namesakes without changing their display names."""
from pathlib import Path
import uuid
import xml.etree.ElementTree as ET
import database as db

class NameInUse(ValueError):
    pass

def reserve_folder(parent, name, confirmed=False):
    parent = Path(parent)
    with db.get_conn() as conn:
        names = [r[0] for r in conn.execute("SELECT folder_name FROM favourite_entities WHERE kind='performer'")]
    taken = {p.name.casefold() for p in parent.iterdir()} if parent.exists() else set()
    duplicate = name.casefold() in {str(n).casefold() for n in names} or name.casefold() in taken
    if duplicate and not confirmed:
        raise NameInUse(f'“{name}” is already in use. Add a different performer with this name in a numbered folder?')
    parent.mkdir(parents=True, exist_ok=True)
    number = 2 if duplicate else 1
    while True:
        label = name if number == 1 else f'{name} ({number})'
        if label.casefold() in taken:
            number += 1
            continue
        try:
            (parent / label).mkdir()
            return parent / label
        except FileExistsError:
            if not confirmed:
                raise NameInUse(f'“{name}” is already in use. Add a different performer in a numbered folder?')
            number += 1

def identify_show(nfo):
    root = ET.fromstring(nfo)
    for entry in root.findall('uniqueid'):
        entry.set('default', 'false')
    ET.SubElement(root, 'uniqueid', {'type':'topshelf', 'default':'true'}).text = str(uuid.uuid4())
    return ET.tostring(root, encoding='unicode')

def preserve_name(row_id, name):
    with db.get_conn() as conn:
        conn.execute('UPDATE favourite_entities SET library_name=?, folder_name=? WHERE id=?', (name,name,row_id))
        conn.commit()

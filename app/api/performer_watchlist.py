from fastapi import APIRouter, Body, HTTPException
import json
import database as db
from app.core import performer_watchlist as watch

router = APIRouter(prefix='/api/performer-watchlist')

@router.get('')
def listing(): return {'items':watch.rows()}

@router.post('')
def add(payload: dict = Body(...)):
    try: values = watch.validate(payload)
    except ValueError as exc: raise HTTPException(400,str(exc))
    with db.get_conn() as c:
        c.execute('BEGIN IMMEDIATE')
        if c.execute('SELECT 1 FROM performer_watchlist WHERE lower(name)=lower(?)',(values[0],)).fetchone():
            raise HTTPException(409,'That name is already on the watchlist')
        identity = c.execute('INSERT INTO performer_watchlist(name,aliases,notes,source_url) VALUES(?,?,?,?)',values).lastrowid
        c.commit()
    watch._wake.set()
    return {'id':identity}

@router.post('/check')
def check_all():
    with db.get_conn() as c:
        c.execute('UPDATE performer_watchlist SET next_check=0 WHERE confirmed IS NULL AND checking=0'); c.commit()
    watch._wake.set()
    return {'ok':True}

@router.put('/{identity}')
def edit(identity: int, payload: dict = Body(...)):
    try: values=watch.validate(payload)
    except ValueError as exc: raise HTTPException(400,str(exc))
    with db.get_conn() as c:
        c.execute('BEGIN IMMEDIATE')
        if c.execute('SELECT 1 FROM performer_watchlist WHERE lower(name)=lower(?) AND id!=?',(values[0],identity)).fetchone():
            raise HTTPException(409,'That name is already on the watchlist')
        row=c.execute('SELECT * FROM performer_watchlist WHERE id=?',(identity,)).fetchone()
        if not row: raise HTTPException(404,'Entry not found')
        try: details=watch.validate_details(payload.get('details', json.loads(row['details'])))
        except ValueError as exc: raise HTTPException(400,str(exc))
        changed = values[0] != row['name'] or json.loads(values[1]) != json.loads(row['aliases'])
        c.execute('UPDATE performer_watchlist SET name=?,aliases=?,notes=?,source_url=?,details=? WHERE id=?',
                  (*values,json.dumps(details),identity))
        if changed:
            c.execute("UPDATE performer_watchlist SET revision=revision+1,candidates='[]',checks='{}',confirmed=NULL,linked_profiles='{}',checked_at=NULL,next_check=0,checking=0 WHERE id=?",(identity,))
        c.commit()
    watch._wake.set()
    return {'ok':True}

@router.post('/{identity}/check')
def check(identity:int):
    with db.get_conn() as c:
        c.execute('UPDATE performer_watchlist SET next_check=0 WHERE id=? AND checking=0 AND confirmed IS NULL',(identity,)); c.commit()
    watch._wake.set()
    return {'ok':True}

@router.post('/{identity}/confirm')
def confirm(identity:int,payload:dict=Body(...)):
    with db.get_conn() as c:
        c.execute('BEGIN IMMEDIATE')
        row=c.execute('SELECT candidates,name,aliases,origin_sources,linked_profiles FROM performer_watchlist WHERE id=?',(identity,)).fetchone()
        if not row: raise HTTPException(404,'Entry not found')
        candidate=next((h for h in json.loads(row[0]) if h['source']==payload.get('source') and h['id']==payload.get('id')),None)
        if candidate and candidate.get('source') not in (*watch.SOURCES, *watch.EXTERNAL_SOURCES): candidate = None
        if candidate and not watch.matches_names(candidate, [row[1]] + json.loads(row[2])): candidate = None
        if not candidate: raise HTTPException(409,'Candidate changed; check again')
        origins=sorted(set(json.loads(row[3])) | {candidate['source']})
        links=json.loads(row[4]);links[candidate['source']]=candidate
        if candidate['source'] in watch.EXTERNAL_SOURCES:
            c.execute('UPDATE performer_watchlist SET linked_profiles=?,origin_sources=?,revision=revision+1,checking=0 WHERE id=?',(json.dumps(links),json.dumps(origins),identity))
        else:
            c.execute('UPDATE performer_watchlist SET confirmed=?,linked_profiles=?,origin_sources=?,revision=revision+1,checking=0 WHERE id=?',(json.dumps(candidate),json.dumps(links),json.dumps(origins),identity))
        if candidate.get('image'):
            saved=json.loads(c.execute('SELECT details FROM performer_watchlist WHERE id=?',(identity,)).fetchone()[0])
            if candidate['source'] in watch.SOURCES or not saved.get('image_url'):
                saved['image_url']=candidate['image']
                c.execute('UPDATE performer_watchlist SET details=? WHERE id=?',(json.dumps(saved),identity))
        c.commit()
    watch._wake.set()
    return {'ok':True}

@router.delete('/{identity}')
def remove(identity:int):
    with db.get_conn() as c:
        c.execute('DELETE FROM performer_watchlist WHERE id=?',(identity,)); c.commit()
    return {'ok':True}


@router.post('/{identity}/details')
def details(identity:int):
    try: return {'details':watch.hydrate_details(identity)}
    except Exception as exc: raise HTTPException(400,str(exc))

@router.post('/{identity}/submit')
def submit(identity:int,payload:dict=Body(...)):
    try: return watch.submit_draft(identity,payload.get('source'),payload.get('dest_dir'))
    except Exception as exc: raise HTTPException(400,str(exc))

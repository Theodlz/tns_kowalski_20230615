import json
import requests
import requests
import sqlalchemy as sa
import tornado.web
from sqlalchemy.orm import scoped_session, sessionmaker
import conesearch_alchemy as ca

from baselayer.app.env import load_env
from baselayer.app.models import init_db
from skyportal.models import DBSession, Obj
from tqdm import tqdm
import tempfile

from skyportal.utils.calculations import great_circle_distance, radec_str2deg

env, cfg = load_env()

init_db(**cfg['database'])

Session = scoped_session(sessionmaker())

tns_history_url = "https://github.com/Theodlz/tns_kowalski_20230615/raw/main/TNS.json"

def tns_history_upload(tns_history):
    objs_updated = 0
    try:
        if len(tns_history) > 0:
            for tns_obj in tqdm(tns_history, desc="TNS history upload", unit="obj", leave=False, total=len(tns_history)):
                tns_name = str(tns_obj["name"]).strip().replace(" ", "")
                if "AT" in tns_name:
                    tns_name = tns_name.replace("AT", "")
                elif "SN" in tns_name:
                    tns_name = tns_name.replace("SN", "")
                else:
                    pass
                tns_ra, tns_dec = radec_str2deg(tns_obj["ra"], tns_obj["dec"])
                if Session.registry.has():
                    session = Session()
                else:
                    session = Session(bind=DBSession.session_factory.kw["bind"])
                try:
                    other = ca.Point(ra=tns_ra, dec=tns_dec)
                    obj_query = session.scalars(
                        sa.select(Obj).where(
                            Obj.within(other, 0.000555556)  # 2 arcseconds
                        )
                    ).all()
                    if len(obj_query) > 0:
                        closest_obj = obj_query[0]
                        closest_obj_dist = great_circle_distance(
                            tns_ra, tns_dec, closest_obj.ra, closest_obj.dec
                        )
                        for obj in obj_query:
                            dist = great_circle_distance(
                                tns_ra, tns_dec, obj.ra, obj.dec
                            )
                            if dist < closest_obj_dist:
                                closest_obj = obj
                                closest_obj_dist = dist
                        obj = closest_obj
                        if obj.tns_name is None or obj.tns_name == "":
                            obj.tns_name = tns_name
                            session.commit()
                            objs_updated += 1
                            print(f"Updated {obj.id} with TNS name {obj.tns_name}")
                            print(f"ra: {obj.ra}, dec: {obj.dec}, tns_ra: {tns_ra}, tns_dec: {tns_dec}, tns_ra_str: {tns_obj['ra']}, tns_dec_str: {tns_obj['dec']}")
                except Exception as e:
                    print(f"Error adding TNS name to object: {str(e)}")
                    session.rollback()
                finally:
                    session.close()
    except Exception as e:
        print(f"Error getting TNS objects: {str(e)}")

if __name__ == "__main__":
    # try to load the file from disk
    tns_history = []
    try:
        with open("TNS.json", "r") as f:
            tns_history = json.load(f)
    except Exception as e:
        print(f"TNS.json not on disk: {str(e)}")
        tns_history = []
    if len(tns_history) == 0:
        response = requests.get(tns_history_url, allow_redirects=True, timeout=30, stream=True)
        response.raise_for_status()
        total_size_in_bytes= int(response.headers.get('content-length', 0))
        block_size = 1024 #1 Kibibyte
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        tns_history = []
        with tempfile.NamedTemporaryFile() as tmp_file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                tmp_file.write(data)
            progress_bar.close()
            tmp_file.seek(0)
            tns_history = json.loads(tmp_file.read())
        # if we managed to get the entire file, save it to disk
        if len(tns_history) > 0:
            with open("TNS.json", "w") as f:
                json.dump(tns_history, f)

    print(len(tns_history))
    tns_history_upload(tns_history)

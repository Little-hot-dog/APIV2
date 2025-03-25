from fastapi import FastAPI, HTTPException, Path, Query, Body, Depends
from typing import Optional, List, Dict, Annotated, List

from pydantic.v1.datetime_parse import time_re
from sqlalchemy import String
from sqlalchemy.orm import Session

from models import Base, RawData, SystemInfo
from database import engine, session_local
from schemas import RawDataRequest, SystemInfoResponse

from datetime import datetime




app = FastAPI()
Base.metadata.create_all(bind=engine)


def get_db():
    db = session_local()
    try:
        yield db
    finally:
        db.close()


@app.post("/post-data/")
async def create_raw_data(raw_data: RawDataRequest, db: Session = Depends(get_db)):
    for item_data in raw_data.data:
        host = item_data.get('host')
        db_raw_data = RawData(host=host,data=item_data, time_date = datetime.now())
        db.add(db_raw_data)
        db.commit()
        db.refresh(db_raw_data)

        distribution_to_table(db_raw_data.data, db)

    return {"massage": "Post успешно завершен"}

#Производит пересон данных в таблицу system_info
def distribution_to_table(data: dict, db: Session):
    host = data.get("host")
    for param, value in data.items():
        if isinstance(value, list):
            value = str(value)
        sys_info = SystemInfo(host=host, param=param, value=value)
        db.add(sys_info)
    db.commit()

@app.get("/get-data/{host}", response_model=List[SystemInfoResponse])
async def get_data(host: Annotated[str, Path(..., title="Укажите имя host")],
                   db: Session = Depends(get_db)):
    params = db.query(SystemInfo).filter(SystemInfo.host == host).all()
    return params


@app.get("/get-filtered-system-info/", response_model=List[SystemInfoResponse])
async def get_filtred_info(hosts: Optional[List[str]] = Query(default = None, title='Укажите host для фильтрации'),
                           params: Optional[List[str]] = Query(default = None, title='Укажите host для фильтрации'),
                           values: Optional[List[str]] = Query(default = None, title='Укажите host для фильтрации'),
                            start_date: Optional[datetime] = Query(default=None, title='Начальная дата для фильтрации'),
                            end_date: Optional[datetime] = Query(default=None, title='Конечная дата для фильтрации'),
                           db: Session = Depends(get_db)):
    query = db.query(SystemInfo)
    if hosts:
        query = query.filter(SystemInfo.host.in_(hosts))

    if params:
        query = query.filter(SystemInfo.param.in_(params))

    if values:
        query = query.filter(SystemInfo.param.in_(values))

    if start_date:
        query = query.filter(SystemInfo.time_date >= start_date)

    if end_date:
        query = query.filter(SystemInfo.time_date <= end_date)

    return query.all()


@app.delete("/delete-data/{host}")
async def delete_data(host: Annotated[str, Path(..., title="Укажите имя host")],
                      db: Session = Depends(get_db)):
    params = db.query(SystemInfo).filter(SystemInfo.host == host).all()
    if not params:
        raise HTTPException(status_code=404, detail="Такого host в таблице system-info не найдено")
    for param in params:
        db.delete(param)

    params = db.query(RawData).filter(RawData.host == host).all()
    if not params:
        raise HTTPException(status_code=404, detail="Такого host таблице в raw-data не найдено")
    for param in params:
        db.delete(param)

    db.commit()
    return{"massage": f'Удаление {host} успешно завершено'}

@app.delete("/delete-all-data/")
async def delete_all_data(db: Session = Depends(get_db)):
    try:
        # Удаление всех записей из таблицы SystemInfo
        db.query(SystemInfo).delete()

        # Удаление всех записей из таблицы RawData
        db.query(RawData).delete()

        # Подтверждение изменений в базе данных
        db.commit()

        return {"message": "Все данные успешно удалены"}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка при удалении данных: {str(e)}")


from fastapi import Depends, UploadFile, File

import pandas as pd
import json


@app.post("/upload-excel/")
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    df = pd.read_excel(file.file, dtype=str)
    failed_rows = []

    for _, row in df.iterrows():
        json_data = row.get("JSON", "{}")

        if not isinstance(json_data, str):
            json_data = "{}"

        try:
            parsed_data = json.loads(json_data)
        except json.JSONDecodeError as e:
            failed_rows.append({"JSON": json_data, "error": str(e)})
            continue

        host = parsed_data.get("host", "unknown")

        # Записываем в raw_data
        raw_entry = RawData(host=host, data=parsed_data, time_date = datetime.now())
        db.add(raw_entry)
        db.commit()
        db.refresh(raw_entry)

        # Записываем в system_info
        for key, value in parsed_data.items():
            if isinstance(value, (str, int, float)):  # Простые значения
                sys_entry = SystemInfo(host=host, param=key, value=str(value))
                db.add(sys_entry)
            elif isinstance(value, list):  # Массивы - записываем каждый элемент отдельно
                for item in value:
                    sys_entry = SystemInfo(host=host, param=key, value=str(item))
                    db.add(sys_entry)

        db.commit()

    if failed_rows:
        failed_df = pd.DataFrame(failed_rows)
        failed_df.to_csv("failed_rows.csv", index=False)

    return {"message": "File processed successfully", "failed_rows": len(failed_rows)}

#new code critical point
from schemas import CriticalPointCreate, CriticalPointResponse
from models import CriticalPoint

@app.post("/critical-points/", response_model=CriticalPointResponse)
def create_critical_point(critical_point: CriticalPointCreate, db: Session = Depends(get_db)):
    db_critical_point = CriticalPoint(**critical_point.dict())
    db.add(db_critical_point)
    db.commit()
    db.refresh(db_critical_point)
    return db_critical_point

@app.get("/critical-points/", response_model=List[CriticalPointResponse])
def read_critical_points(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return db.query(CriticalPoint).offset(skip).limit(limit).all()

@app.put("/critical-points/{param}", response_model=CriticalPointResponse)
def update_critical_point(param: Annotated[str, Path(..., title="Укажите param")],
                          critical_point: CriticalPointCreate,
                          db: Session = Depends(get_db)):
    db_critical_point = db.query(CriticalPoint).filter(CriticalPoint.param == param).first()
    if db_critical_point is None:
        raise HTTPException(status_code=404, detail="Critical point not found")

    for key, value in critical_point.dict(exclude_unset=True).items():
        setattr(db_critical_point, key, value)

    db.commit()
    db.refresh(db_critical_point)
    return db_critical_point

@app.delete("/critical-points/{param}")
async def delete_critical_point(param: Annotated[str, Path(..., title="Укажите param")],
                                db: Session = Depends(get_db)):
    del_param = db.query(CriticalPoint).filter(CriticalPoint.param == param).first()
    if not del_param:
        raise HTTPException(status_code=404, detail="Такого param не найдено. Возможно его стоит добавить в список критических точек")

    db.delete(del_param)
    db.commit()
    return {"massage": f'Удаление итической точки "{param}" успешно завершено'}


# async def upload_excel(file: UploadFile = File(...), db: Session = Depends(get_db)):
#     if file.content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
#         raise HTTPException(status_code=400, detail="Invalid file type. Please upload an Excel file.")
#
#     # Чтение Excel-файла
#     df = pd.read_excel(file.file, engine='openpyxl')
#
#     for index, row in df.iterrows():
#         try:
#             # data_id = row[0]  # Первый столбец - ID
#             data_json = row[1]  # Второй столбец - данные в JSON формате
#             print(f'Информация {data_json}')
#             # Преобразование строки JSON в словарь
#             data_dict = json.loads(data_json)
#
#             # Создание записи в базе данных
#             raw_data = RawData(host=data_dict.get('host', 'default'), data=data_dict, time_date=datetime.now())
#             db.add(raw_data)
#             db.commit()
#             db.refresh(raw_data)
#
#             # Распределение данных в таблицу system_info
#             distribution_to_table(data_dict, db)
#
#         except json.JSONDecodeError:
#             raise HTTPException(status_code=400, detail=f"Invalid JSON format in row {index + 2}")
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error processing row {index + 2}: {str(e)}")
#
#     return {"message": "Excel file successfully uploaded and data processed"}
# №№№№№№№№№№№№№№№№№№№
# @app.post("/post-raw-data/")
# async def create_raw_data(raw_data: RawDataRequest, db: Session = Depends(get_db)):
#     for item_data in raw_data.data:
#         db_raw_data = RawData(data=item_data, time_date = datetime.now())
#         db.add(db_raw_data)
#         db.commit()
#         db.refresh(db_raw_data)
#
#         distribution_to_table(db_raw_data.data, db)
#
#     return raw_data
#
# @app.get("/get-system-info/")
# async def get_system_info(limit: int = 10, db: Session = Depends(get_db)):
#     data = db.query(SystemInfo).limit(limit).all()
#     return data
#
#
# @app.get("/get-system-info/{id}")
# async def get_system_info(id: Annotated[int, Path(..., title="Укажите id", ge=1)],
#                           db: Session = Depends(get_db)):
#     data = db.query(SystemInfo).filter(SystemInfo.id == id).first()
#     if SystemInfo is None:
#         return HTTPException(status_code=404, detail="Такой id не найден")
#     return data
#
#
# @app.delete("/delete-raw-and-system-info/{id}")
# async def delete_data(id: Annotated[int, Path(..., title="Укажите id", ge=1)],
#                       db: Session = Depends(get_db)):
#     sys_info = db.query(SystemInfo).filter(SystemInfo.id == id).first()
#     if sys_info:
#         db.delete(sys_info)
#         db.commit()
#     else:
#         raise HTTPException(status_code=404, detail="Такой id в таблице raw-info не найден")
#
#     raw_data = db.query(RawData).filter(RawData.id == id).first()
#     if raw_data:
#         db.delete(raw_data)
#         db.commit()
#     else:
#         raise HTTPException(status_code=404, detail="Такой id в таблице system-info не найден")



# def distribution_to_table(data: dict, db: Session):
#     system_info = SystemInfo(
#         host = data.get('host', ''),
#         dhcp_addr = data.get('dhcp_addr', ''),
#         kav_ver= data.get('kav_ver', ''),  # A
#         kav_base = data.get('kav_base', ''), # A
#         ufw_on = data.get('ufw_on', ''),  # A
#         ufw_rules = data.get('ufw_rules', ''), # L
#         shares = data.get('shares', ''),  # A
#         samba = data.get('samba', ''),  # L
#         pw_rules = data.get('pw_rules', ''),  # L
#         pw_deny = data.get('pw_deny', ''),   #L
#         ssh_root = data.get('ssh_root', ''),   #LI
#         ssh_allow = data.get('ssh_allow', ''),   #L
#         sudo_pw = data.get('sudo_pw', ''),   #A
#         pw_empty = data.get('pw_empty', ''),   #L
#         syn = data.get('syn', ''),  #L
#         ipv6 = data.get('ipv6', ''),   #A
#         telnet = data.get('telnet', ''),   #L
#         r7_ver = data.get('r7_ver', ''), # A
#         last_in = data.get('last_in', ''),  #L
#         last_out = data.get('last_out', ''),  #L
#         dns = data.get('dns', ''),  #A
#         cdrom = data.get('cdrom', ''),  #A
#         pw_limit = data.get('pw_limit', ''),  #A
#         pw_days = data.get('pw_days', ''),  #A
#         pw_change = data.get('pw_change', ''),  #A
#         last_boot = data.get('last_boot', ''),  #A
#         board = data.get('board', ''),  #A
#         cpu = data.get('cpu', ''),  #A
#         mac = data.get('mac', ''),  #A
#         mem = data.get('mem', []),  #A Может хранить массив
#
#         noautorun = data.get('noautorun', ''),  #W
#         kav_srv = data.get('kav_srv', ''),  #A
#         sysdisk = data.get('sysdisk', ''),  #W
#         pdisk = data.get('pdisk', []),  #A Может хранить массив
#         lan = data.get('lan', []),  #A Может хранить массив
#         osver = data.get('osver', ''), #A
#         kav_svc = data.get('kav_svc', ''),  #A
#         indsvc = data.get('indsvc', ''), #W
#         userlogf = data.get('userlogf', ''),  #A
#         la = data.get('la', ''), #W
#         pwdlimit_ad = data.get('pwdlimit_ad', ''),  #W
#         pwdlast = data.get('pwdlast', ''), #W
#         inet = data.get('inet', ''),  #A
#         crypto = data.get('crypto', ''),  #A
#         zip = data.get('zip', ''),  #W
#         adobe = data.get('adobe', ''),  #W
#         lua = data.get('lua', ''),  #W
#         root_size = data.get('root_size', ''),  #L
#         root_free = data.get('root_free', ''),  #A
#         kaa_ver = data.get('kaa_ver', ''),  #A
#         kav_scan = data.get('kav_scan', ''),  #A
#         kaa_svc = data.get('kaa_svc', ''),  #A
#         policy = data.get('policy', ''),  #A
#         ssh = data.get('ssh', ''),  #L
#         ldiskc = data.get('ldiskc', ''),  #W
#         pkg_upg = data.get('pkg_upg', ''),  #L
#         r7_org = data.get('r7_org', ''),  #L
#         litoria = data.get('litoria', ''),  #L
#         kernel = data.get('kernel', ''),  #L
#         ad_join = data.get('ad_join', ''),  #L
#         libre = data.get('libre', ''),  #L
#         wine = data.get('wine', ''),  #L
#         yandex = data.get('yandex', ''),  #L
#         vkteams = data.get('vkteams', ''),  #L
#         firefox = data.get('firefox', ''),  #L
#         scrn = data.get('scrn', '')  #A
#     )
#     db.add(system_info)
#     db.commit()
#     db.refresh(system_info)



# @app.post("/post-raw-data/")
# async def create_raw_data(raw_data: RawDataRequest, db: Session = Depends(get_db)):
#
#     db_raw_data = RawData(data=raw_data.data)
#     db.add(db_raw_data)
#     db.commit()
#     db.refresh(db_raw_data)
#
#     distribution_to_table(db_raw_data.data, db)
#
#     return raw_data
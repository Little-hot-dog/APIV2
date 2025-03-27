from fastapi import FastAPI, HTTPException, Path, Query, Body, Depends, UploadFile, File
from typing import Optional, Annotated, List
from sqlalchemy.orm import Session
from models import Base, RawData, SystemInfo
from database import engine, session_local
from schemas import RawDataRequest, SystemInfoResponse, RawDataUpdate
from datetime import datetime
import pandas as pd
import json

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

@app.put("/update-data/{host}")
async def update_raw_data(host: Annotated[str, Path(..., title="Укажите имя host")],
                          updated_data: RawDataUpdate,
                          db: Session = Depends(get_db)):
    # Поиск существующей записи в таблице RawData
    db_raw_data = db.query(RawData).filter(RawData.host == host).first()
    if not db_raw_data:
        raise HTTPException(status_code=404, detail="Такой host не найден")

    # Обновление данных
    db_raw_data.data = updated_data.data
    db_raw_data.time_date = datetime.now()

    db.commit()
    db.refresh(db_raw_data)

    # Обновление данных в таблице SystemInfo
    update_to_table(db_raw_data.data, db)

    return {"message": f"Данные ПК {host} успешно обновлены"}

def update_to_table(data: dict, db: Session):
    host = data.get("host")
    # Удаление старых записей для данного host
    db.query(SystemInfo).filter(SystemInfo.host == host).delete()

    # Добавление новых записей
    for param, value in data.items():
        if isinstance(value, list):
            value = str(value)
        sys_info = SystemInfo(host=host, param=param, value=value)
        db.add(sys_info)
    db.commit()

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

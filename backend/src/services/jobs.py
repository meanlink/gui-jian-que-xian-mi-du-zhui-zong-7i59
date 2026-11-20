import time
from fastapi import HTTPException
from redis import Redis
from sqlalchemy.exc import IntegrityError
from src.config.settings import REDIS_URL
from src.models import Dataset, Job
from src.services.business import metadata
from src.services import calculations
def normalize_params(raw):
    allowed={p['key']:p for p in metadata().get('parameters',[])}
    if set(raw)-set(allowed): raise HTTPException(422,'包含未知分析参数')
    params={k:p['default'] for k,p in allowed.items()}
    params.update(raw)
    for key,p in allowed.items():
        v=params[key]
        if p['type']=='number':
            try:
                v=float(v)
                if not (p.get('min',-1e9)<=v<=p.get('max',1e9)): raise ValueError()
                params[key]=v
            except (ValueError,TypeError): raise HTTPException(422, f'{p["label"]} 超出范围或非数值')
        elif str(v) not in [str(o['value']) for o in p.get('options',[])]:
            raise HTTPException(422,f'{p["label"]} 不是有效选项')
    return params
def create(db, data, user):
    dataset=db.get(Dataset,data.dataset_id)
    if not dataset: raise HTTPException(404,'数据批次不存在')
    params=normalize_params(data.params)
    for _ in range(3):
        try:
            with db.begin_nested():
                calc,created=calculations.attach(db,dataset,params)
                j=Job(name=data.name, dataset_id=dataset.id, params=params, created_by=user.id,
                      created_at=time.time(), calculation_id=calc.id, status=calc.status)
                if calc.status=='completed': # 复用已完成的计算，结果立即可用
                    j.result=calc.result; j.finished_at=calc.finished_at
                db.add(j); db.flush()
            return j, calc, created
        except IntegrityError:
            pass # 并发建试验撞上同一轮计算，重试即共用对方那一轮
    raise HTTPException(409,'计算登记冲突，请重试')
def enqueue(calculation_id):
    try:
        r=Redis.from_url(REDIS_URL); r.rpush('analysis:calculations',calculation_id); r.close()
    except Exception:
        pass # SQL queued status is the durable fallback

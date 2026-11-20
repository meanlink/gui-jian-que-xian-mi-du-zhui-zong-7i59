"""同批内容与实际参数一致的试验共用一次计算；最新一轮失败后，新试验开启下一轮。"""
import time
from sqlalchemy import select
from src.models import Calculation
from src.utils.serialization import checksum
def fingerprint(dataset, params):
    return checksum({'dataset': dataset.checksum, 'params': params})
def attach(db, dataset, params):
    """返回 (calculation, created)。优先复用已完成计算，其次共用排队/计算中的那一轮；
    没有任何未失败的一轮时，开启新一轮 epoch。"""
    fp = fingerprint(dataset, params)
    calcs = db.scalars(select(Calculation).where(Calculation.fingerprint == fp).order_by(Calculation.epoch.desc())).all()
    completed = next((c for c in calcs if c.status == 'completed'), None)
    if completed:
        return completed, False
    inflight = next((c for c in calcs if c.status in ('queued', 'running')), None)
    if inflight:
        return inflight, False
    calc = Calculation(fingerprint=fp, epoch=(calcs[0].epoch + 1) if calcs else 1,
                       dataset_id=dataset.id, params=params, status='queued', created_at=time.time())
    db.add(calc); db.flush()
    return calc, True

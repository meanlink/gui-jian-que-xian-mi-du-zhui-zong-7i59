from sqlalchemy import inspect, text, select
from src.config.database import Base, engine, Session
from src import models
from src.models import Job, Dataset, Calculation
def migrate():
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        cols=[c['name'] for c in inspect(conn).get_columns('jobs')]
        if 'calculation_id' not in cols:
            conn.execute(text('ALTER TABLE jobs ADD COLUMN calculation_id INTEGER'))
    backfill()
def backfill():
    """为迁移前的试验补登记计算：每个历史试验各自一轮，状态与结果原样保留。"""
    from src.services.calculations import fingerprint
    with Session() as db:
        epochs={}
        for fp,epoch in db.execute(select(Calculation.fingerprint,Calculation.epoch)):
            epochs[fp]=max(epochs.get(fp,0),epoch)
        orphans=list(db.scalars(select(Job).where(Job.calculation_id==None).order_by(Job.id)))
        for j in orphans:
            d=db.get(Dataset,j.dataset_id)
            if not d: continue
            fp=fingerprint(d,j.params or {})
            epochs[fp]=epochs.get(fp,0)+1
            status=j.status if j.status in ('queued','completed','failed') else 'queued'
            calc=Calculation(fingerprint=fp,epoch=epochs[fp],dataset_id=d.id,params=j.params or {},
                             status=status,attempt=1 if status in ('completed','failed') else 0,
                             result=j.result or {},error=j.error or '',
                             created_at=j.created_at,finished_at=j.finished_at)
            db.add(calc); db.flush()
            j.calculation_id=calc.id
            if j.status=='running': j.status='queued' # 中断的执行由 worker 按同一轮计算恢复
        db.commit()
if __name__=='__main__':migrate()

import time, json, logging
from pathlib import Path
from sqlalchemy import select, update
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report, Calculation
from src.services.business import analyze
from src.services.reports import deliver
from src.services.audit import record
logging.basicConfig(level=logging.INFO)
def claim(db, calc_id):
    """原子认领：仅排队中的计算可被认领；attempt 递增作为本次执行的令牌。"""
    attempt=db.scalar(update(Calculation).where(Calculation.id==calc_id,Calculation.status=='queued')
                      .values(status='running',attempt=Calculation.attempt+1).returning(Calculation.attempt))
    db.commit()
    return attempt
def finalize(db, calc_id, attempt, status, result=None, error=''):
    """按令牌落库并唤醒所有等候试验；令牌不符说明是旧进程迟到的结果，直接作废。"""
    finished=time.time()
    values={'status':status,'finished_at':finished}
    if result is not None: values['result']=result
    if error: values['error']=error
    matched=db.execute(update(Calculation).where(Calculation.id==calc_id,Calculation.status=='running',Calculation.attempt==attempt)
                       .values(**values)).rowcount
    if matched!=1:
        db.rollback()
        logging.warning('Discarded stale result for calculation %s attempt %s',calc_id,attempt)
        return False
    job_values={'status':status,'finished_at':finished}
    if result is not None: job_values['result']=result
    if error: job_values['error']=error
    db.execute(update(Job).where(Job.calculation_id==calc_id,Job.status.in_(['queued','running'])).values(**job_values))
    return True
def execute(calc_id):
    with Session() as db:
        attempt=claim(db,calc_id)
        if attempt is None:return
        calc=db.get(Calculation,calc_id)
        try:
            d=db.get(Dataset,calc.dataset_id)
            result=analyze(d.records,calc.params)
            json.dumps(result,allow_nan=False)
            if finalize(db,calc_id,attempt,'completed',result=result):
                record(db,'worker','calculation.completed',{'id':calc_id})
        except Exception as e:
            logging.exception('Analysis failed')
            if finalize(db,calc_id,attempt,'failed',error=str(e)[:990]):
                record(db,'worker','calculation.failed',{'id':calc_id,'error':str(e)[:990]})
        db.commit()
def tick():
    with Session() as db:
        queued=list(db.scalars(select(Calculation.id).where(Calculation.status=='queued').limit(3)))
    for calc_id in queued:execute(calc_id)
    with Session() as db:
        for r in db.scalars(select(Report).where(Report.enabled==True,Report.next_run<=time.time())):
            d=deliver(db,r)
            if d:record(db,'worker','report.delivered',{'id':r.id,'delivery':d.id})
        db.commit()
def recover(db):
    """中断后恢复的是同一轮计算：原 calculation 重新排队即可，重试不新增计算。"""
    for c in db.scalars(select(Calculation).where(Calculation.status=='running')):
        c.status='queued'
        db.execute(update(Job).where(Job.calculation_id==c.id,Job.status=='running').values(status='queued'))
        record(db,'worker','calculation.recovered',{'id':c.id})
    db.commit()
def main():
    r=Redis.from_url(REDIS_URL,socket_timeout=3)
    # This Compose stack has a single worker. A crash must not strand its jobs.
    with Session() as db:recover(db)
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            item=r.blpop('analysis:calculations',timeout=1)
            if item:execute(int(item[1]))
            tick()
        except Exception:
            logging.exception('Worker retry');time.sleep(2)
if __name__=='__main__':main()

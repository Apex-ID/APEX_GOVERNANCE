import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'apex_project.settings')
app = Celery('apex_project')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

app.conf.beat_schedule = {
    'executar-pipeline-diariamente': {
        'task': 'qualidade_ad.tasks.executar_pipeline_completo_task',
        'schedule': crontab(hour=2, minute=0), #Todo dia às 2:00 
    },
}
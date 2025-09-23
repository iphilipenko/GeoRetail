"""
Celery configuration for batch processing
"""

from celery import Celery
from kombu import Queue
import os

# Create Celery app
app = Celery('georetail_screening')

# Configure from environment or defaults
app.conf.update(
    broker_url=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/1'),
    result_backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/2'),
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    task_routes={
        'screening.batch.*': {'queue': 'batch_processing'},
        'screening.export.*': {'queue': 'export_tasks'},
    },
    task_queues=(
        Queue('batch_processing', routing_key='batch.#'),
        Queue('export_tasks', routing_key='export.#'),
    ),
    task_default_queue='default',
    task_default_exchange='tasks',
    task_default_routing_key='task.default',
)

# Auto-discover tasks
app.autodiscover_tasks(['src.api.v2.uc_screening.batch'])

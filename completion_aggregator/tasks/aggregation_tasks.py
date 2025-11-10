"""
Asynchronous tasks for performing aggregation of completions.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import time

from celery import shared_task
from celery_utils.logged_task import LoggedTask
from opaque_keys.edx.keys import CourseKey, UsageKey

from django.contrib.auth import get_user_model
from django.db import connection

from .. import core
from ..models import StaleCompletion

User = get_user_model()

UPDATE_SQL = """
UPDATE completion_blockcompletion completion, progress_coursemodulecompletion progress
   SET completion.created = progress.created,
       completion.modified = progress.modified
 WHERE completion.user_id = progress.user_id
   AND completion.block_key = progress.content_id
   AND completion.course_key = progress.course_id
   AND completion.id IN %(ids)s;
"""

log = logging.getLogger(__name__)


def enqueue_user_aggregation(username, course_key, block_key=None):
    """
    Enqueue aggregation task for a single user/course without batch processing overhead.

    This is used by the sync mode signal handler to immediately enqueue a task
    after a BlockCompletion is saved, bypassing the batch processing logic and
    cache locking in perform_aggregation().

    Parameters
    ----------
        username (str):
            The user whose aggregators need updating.
        course_key (str or CourseKey):
            The course in which the aggregators need updating.
        block_key (str or UsageKey, optional):
            The specific block that changed. Can be None for course-wide updates.
    """
    from django.conf import settings

    # Convert to string if necessary
    course_key_str = str(course_key)
    block_keys = [str(block_key)] if block_key else []

    # Prepare task options
    task_options = {}
    routing_key = getattr(settings, 'COMPLETION_AGGREGATOR_ROUTING_KEY', None)
    if routing_key:
        task_options['routing_key'] = routing_key

    # Enqueue the task
    log.info(
        "Enqueueing aggregation task for user %s in course %s (block: %s)",
        username, course_key_str, block_key
    )

    update_aggregators.apply_async(
        kwargs={
            'username': username,
            'course_key': course_key_str,
            'block_keys': block_keys,
            'force': False,
        },
        **task_options
    )


@shared_task(base=LoggedTask)
def update_aggregators(username, course_key, block_keys=(), force=False):
    """
    Update aggregators for the specified enrollment (user + course).

    Parameters
    ----------
        username (str):
            The user whose aggregators need updating.
        course_key (str):
            The course in which the aggregators need updating.
        block_key (list[str]):
            A list of completable blocks that have changed.
        force (bool):
            If True, update aggregators even if they are up-to-date.

    Takes a collection of block_keys that have been updated, to enable future
    optimizations in how aggregators are recalculated.
    """
    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        log.warning("User %s does not exist.  Marking stale completions resolved.", username)
        StaleCompletion.objects.filter(username=username).update(resolved=True)
        return None

    course_key = CourseKey.from_string(course_key)
    block_keys = set(UsageKey.from_string(key).map_into_course(course_key) for key in block_keys)
    log.info(
        "Updating aggregators in %s for %s. Changed blocks: %s", course_key, user.username, block_keys,
    )
    return core.update_aggregators(user, course_key, block_keys, force)


@shared_task
def migrate_batch(batch_size, delay_between_tasks):
    """
    Wraps _migrate_batch to simplify testing.
    """
    _migrate_batch(batch_size, delay_between_tasks)


def _migrate_batch(batch_size, delay_between_tasks):
    """
    Convert a batch of CourseModuleCompletions to BlockCompletions.

    Given a starting ID and a stopping ID, this task will:

    * Fetch all CourseModuleCompletions with an ID in range(start_id, stop_id).
    * Update the BlockCompletion table with those CourseModuleCompletion
      records.
    """

    def get_next_id_batch():
        while True:
            with connection.cursor() as cur:
                count = cur.execute(
                    """
                    SELECT id
                    FROM completion_blockcompletion
                    WHERE NOT completion_blockcompletion.modified
                    LIMIT %(batch_size)s;
                    """,
                    {"batch_size": batch_size},
                )
                ids = [row[0] for row in cur.fetchall()]
                if count == 0:
                    break
            yield ids

    with connection.cursor() as cur:
        count = 0
        for ids in get_next_id_batch():
            count = cur.execute(UPDATE_SQL, {"ids": ids},)
            time.sleep(delay_between_tasks)
        log.info("Completed progress updation batch of %s objects", count)

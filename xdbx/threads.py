"""
NoSQLite3 SqliteMutiThread Class

Derived(more like shamelessly copied) from sqlitedict

                              Apache License
                        Version 2.0, January 2004
                     http://www.apache.org/licenses/

TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

1. Definitions.

   "License" shall mean the terms and conditions for use, reproduction,
   and distribution as defined by Sections 1 through 9 of this document.

   "Licensor" shall mean the copyright owner or entity authorized by
   the copyright owner that is granting the License.

   "Legal Entity" shall mean the union of the acting entity and all
   other entities that control, are controlled by, or are under common
   control with that entity. For the purposes of this definition,
   "control" means (i) the power, direct or indirect, to cause the
   direction or management of such entity, whether by contract or
   otherwise, or (ii) ownership of fifty percent (50%) or more of the
   outstanding shares, or (iii) beneficial ownership of such entity.

   "You" (or "Your") shall mean an individual or Legal Entity
   exercising permissions granted by this License.

   "Source" form shall mean the preferred form for making modifications,
   including but not limited to software source code, documentation
   source, and configuration files.

   "Object" form shall mean any form resulting from mechanical
   transformation or translation of a Source form, including but
   not limited to compiled object code, generated documentation,
   and conversions to other media types.

   "Work" shall mean the work of authorship, whether in Source or
   Object form, made available under the License, as indicated by a
   copyright notice that is included in or attached to the work
   (an example is provided in the Appendix below).

   "Derivative Works" shall mean any work, whether in Source or Object
   form, that is based on (or derived from) the Work and for which the
   editorial revisions, annotations, elaborations, or other modifications
   represent, as a whole, an original work of authorship. For the purposes
   of this License, Derivative Works shall not include works that remain
   separable from, or merely link (or bind by name) to the interfaces of,
   the Work and Derivative Works thereof.

   "Contribution" shall mean any work of authorship, including
   the original version of the Work and any modifications or additions
   to that Work or Derivative Works thereof, that is intentionally
   submitted to Licensor for inclusion in the Work by the copyright owner
   or by an individual or Legal Entity authorized to submit on behalf of
   the copyright owner. For the purposes of this definition, "submitted"
   means any form of electronic, verbal, or written communication sent
   to the Licensor or its representatives, including but not limited to
   communication on electronic mailing lists, source code control systems,
   and issue tracking systems that are managed by, or on behalf of, the
   Licensor for the purpose of discussing and improving the Work, but
   excluding communication that is conspicuously marked or otherwise
   designated in writing by the copyright owner as "Not a Contribution."

   "Contributor" shall mean Licensor and any individual or Legal Entity
   on behalf of whom a Contribution has been received by Licensor and
   subsequently incorporated within the Work.

2. Grant of Copyright License. Subject to the terms and conditions of
   this License, each Contributor hereby grants to You a perpetual,
   worldwide, non-exclusive, no-charge, royalty-free, irrevocable
   copyright license to reproduce, prepare Derivative Works of,
   publicly display, publicly perform, sublicense, and distribute the
   Work and such Derivative Works in Source or Object form.

3. Grant of Patent License. Subject to the terms and conditions of
   this License, each Contributor hereby grants to You a perpetual,
   worldwide, non-exclusive, no-charge, royalty-free, irrevocable
   (except as stated in this section) patent license to make, have made,
   use, offer to sell, sell, import, and otherwise transfer the Work,
   where such license applies only to those patent claims licensable
   by such Contributor that are necessarily infringed by their
   Contribution(s) alone or by combination of their Contribution(s)
   with the Work to which such Contribution(s) was submitted. If You
   institute patent litigation against any entity (including a
   cross-claim or counterclaim in a lawsuit) alleging that the Work
   or a Contribution incorporated within the Work constitutes direct
   or contributory patent infringement, then any patent licenses
   granted to You under this License for that Work shall terminate
   as of the date such litigation is filed.

4. Redistribution. You may reproduce and distribute copies of the
   Work or Derivative Works thereof in any medium, with or without
   modifications, and in Source or Object form, provided that You
   meet the following conditions:

   (a) You must give any other recipients of the Work or
       Derivative Works a copy of this License; and

   (b) You must cause any modified files to carry prominent notices
       stating that You changed the files; and

   (c) You must retain, in the Source form of any Derivative Works
       that You distribute, all copyright, patent, trademark, and
       attribution notices from the Source form of the Work,
       excluding those notices that do not pertain to any part of
       the Derivative Works; and

   (d) If the Work includes a "NOTICE" text file as part of its
       distribution, then any Derivative Works that You distribute must
       include a readable copy of the attribution notices contained
       within such NOTICE file, excluding those notices that do not
       pertain to any part of the Derivative Works, in at least one
       of the following places: within a NOTICE text file distributed
       as part of the Derivative Works; within the Source form or
       documentation, if provided along with the Derivative Works; or,
       within a display generated by the Derivative Works, if and
       wherever such third-party notices normally appear. The contents
       of the NOTICE file are for informational purposes only and
       do not modify the License. You may add Your own attribution
       notices within Derivative Works that You distribute, alongside
       or as an addendum to the NOTICE text from the Work, provided
       that such additional attribution notices cannot be construed
       as modifying the License.

   You may add Your own copyright statement to Your modifications and
   may provide additional or different license terms and conditions
   for use, reproduction, or distribution of Your modifications, or
   for any such Derivative Works as a whole, provided Your use,
   reproduction, and distribution of the Work otherwise complies with
   the conditions stated in this License.

5. Submission of Contributions. Unless You explicitly state otherwise,
   any Contribution intentionally submitted for inclusion in the Work
   by You to the Licensor shall be under the terms and conditions of
   this License, without any additional terms or conditions.
   Notwithstanding the above, nothing herein shall supersede or modify
   the terms of any separate license agreement you may have executed
   with Licensor regarding such Contributions.

6. Trademarks. This License does not grant permission to use the trade
   names, trademarks, service marks, or product names of the Licensor,
   except as required for reasonable and customary use in describing the
   origin of the Work and reproducing the content of the NOTICE file.

7. Disclaimer of Warranty. Unless required by applicable law or
   agreed to in writing, Licensor provides the Work (and each
   Contributor provides its Contributions) on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied, including, without limitation, any warranties or conditions
   of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
   PARTICULAR PURPOSE. You are solely responsible for determining the
   appropriateness of using or redistributing the Work and assume any
   risks associated with Your exercise of permissions under this License.

8. Limitation of Liability. In no event and under no legal theory,
   whether in tort (including negligence), contract, or otherwise,
   unless required by applicable law (such as deliberate and grossly
   negligent acts) or agreed to in writing, shall any Contributor be
   liable to You for damages, including any direct, indirect, special,
   incidental, or consequential damages of any character arising as a
   result of this License or out of the use or inability to use the
   Work (including but not limited to damages for loss of goodwill,
   work stoppage, computer failure or malfunction, or any and all
   other commercial damages or losses), even if such Contributor
   has been advised of the possibility of such damages.

9. Accepting Warranty or Additional Liability. While redistributing
   the Work or Derivative Works thereof, You may choose to offer,
   and charge a fee for, acceptance of support, warranty, indemnity,
   or other liability obligations and/or rights consistent with this
   License. However, in accepting such obligations, You may act only
   on Your own behalf and on Your sole responsibility, not on behalf
   of any other Contributor, and only if You agree to indemnify,
   defend, and hold each Contributor harmless for any liability
   incurred by, or claims asserted against, such Contributor by reason
   of your accepting any such warranty or additional liability.

END OF TERMS AND CONDITIONS

APPENDIX: How to apply the Apache License to your work.

   To apply the Apache License to your work, attach the following
   boilerplate notice, with the fields enclosed by brackets "[]"
   replaced with your own identifying information. (Don't include
   the brackets!)  The text should be enclosed in the appropriate
   comment syntax for the file format. We also recommend that a
   file or class name and description of purpose be included on the
   same "printed page" as the copyright notice for easier
   identification within third-party archives.

Copyright (c) 2011-now `Radim Řehůřek <http://radimrehurek.com>`_ and contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# Direct Imports
import sqlite3

from .logger import logging
from threading import Thread
from queue import Queue
import traceback
import sys


def reraise(tp, value, tb=None):
    if value is None:
        value = tp()
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


class SqliteMultiThread(Thread):
    """
    Wrap sqlite connection in a way that allows concurrent requests from
    multiple threads.

    This is done by internally queueing the requests and processing them
    sequentially in a separate thread (in the same order they arrived).

    """
    def __init__(self, filename, autocommit, journal_mode, timeout):
        super(SqliteMultiThread, self).__init__()
        self.filename = filename
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        # use request queue of unlimited size
        self.reqs = Queue()
        self.daemon = True
        self.exception = None
        self._sqlitedict_thread_initialized = None
        self.timeout = timeout
        self.log = logging.getLogger('sqlitedict.SqliteMultithread')
        self.start()

    def run(self):
        try:
            if self.autocommit:
                conn = sqlite3.connect(
                    self.filename,
                    isolation_level=None,
                    check_same_thread=False
                )
            else:
                conn = sqlite3.connect(
                    self.filename,
                    check_same_thread=False
                )
        except Exception:
            self.log.exception(
                f"Failed to initialize connection for filename: {self.filename}"
            )
            self.exception = sys.exc_info()
            raise

        try:
            conn.execute(f'PRAGMA journal_mode = {self.journal_mode}')
            conn.text_factory = str
            cursor = conn.cursor()
            conn.commit()
            cursor.execute('PRAGMA synchronous=OFF')
        except Exception:
            self.log.exception("Failed to execute PRAGMA statements.")
            self.exception = sys.exc_info()
            raise

        self._sqlitedict_thread_initialized = True

        res = None
        while True:
            req, arg, res, outer_stack = self.reqs.get()
            if req == '--close--':
                assert res, ('--close-- without return queue', res)
                break
            elif req == '--commit--':
                conn.commit()
                if res:
                    res.put('--no more--')
            else:
                try:
                    cursor.execute(req, arg)
                except Exception:
                    self.exception = (e_type, e_value, e_tb) = sys.exc_info()
                    inner_stack = traceback.extract_stack()

                    # An exception occurred in our thread, but we may not
                    # immediately able to throw it in our calling thread, if it has
                    # no return `res` queue: log as level ERROR both the inner and
                    # outer exception immediately.
                    #
                    # Any iteration of res.get() or any next call will detect the
                    # inner exception and re-raise it in the calling Thread; though
                    # it may be confusing to see an exception for an unrelated
                    # statement, an ERROR log statement from the 'sqlitedict.*'
                    # namespace contains the original outer stack location.
                    self.log.error('Inner exception:')
                    for item in traceback.format_list(inner_stack):
                        self.log.error(item)
                    self.log.error('')  # deliniate traceback & exception w/blank line
                    for item in traceback.format_exception_only(e_type, e_value):
                        self.log.error(item)

                    self.log.error('')  # exception & outer stack w/blank line
                    self.log.error('Outer stack:')
                    for item in traceback.format_list(outer_stack):
                        self.log.error(item)
                    self.log.error('Exception will be re-raised at next call.')

                if res:
                    for rec in cursor:
                        res.put(rec)
                    res.put('--no more--')

                if self.autocommit:
                    conn.commit()

        self.log.debug('received: %s, send: --no more--', req)
        conn.close()
        res.put('--no more--')

    def check_raise_error(self):
        """
        Check for and raise exception for any previous sqlite query.

        For the `execute*` family of method calls, such calls are non-blocking and any
        exception raised in the thread cannot be handled by the calling Thread (usually
        MainThread).  This method is called on `close`, and prior to any subsequent
        calls to the `execute*` methods to check for and raise an exception in a
        previous call to the MainThread.
        """
        if self.exception:
            e_type, e_value, e_tb = self.exception

            # clear self.exception, if the caller decides to handle such
            # exception, we should not repeatedly re-raise it.
            self.exception = None

            self.log.error('An exception occurred from a previous statement, view '
                           'the logging namespace "sqlitedict" for outer stack.')

            # The third argument to raise is the traceback object, and it is
            # substituted instead of the current location as the place where
            # the exception occurred, this is so that when using debuggers such
            # as `pdb', or simply evaluating the naturally raised traceback, we
            # retain the original (inner) location of where the exception
            # occurred.
            reraise(e_type, e_value, e_tb)

    def execute(self, req, arg=None, res=None):
        """
        `execute` calls are non-blocking: just queue up the request and return immediately.
        """
        self._wait_for_initialization()
        self.check_raise_error()

        # NOTE: This might be a lot of information to pump into an input
        # queue, affecting performance.  I've also seen earlier versions of
        # jython take a severe performance impact for throwing exceptions
        # so often.
        stack = traceback.extract_stack()[:-1]
        self.reqs.put((req, arg or tuple(), res, stack))

    def executemany(self, req, items):
        for item in items:
            self.execute(req, item)
        self.check_raise_error()

    def select(self, req, arg=None):
        """
        Unlike sqlite's native select, this select doesn't handle iteration efficiently.

        The result of `select` starts filling up with values as soon as the
        request is dequeued, and although you can iterate over the result normally
        (`for res in self.select(): ...`), the entire result will be in memory.
        """
        res = Queue()  # results of the select will appear as items in this queue
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            self.check_raise_error()
            if rec == '--no more--':
                break
            yield rec

    def select_one(self, req, arg=None):
        """Return only the first row of the SELECT, or None if there are no matching rows."""
        try:
            return next(iter(self.select(req, arg)))
        except StopIteration:
            return None

    def commit(self, blocking=True):
        if blocking:
            # by default, we await completion of commit() unless
            # blocking=False.  This ensures any available exceptions for any
            # previous statement are thrown before returning, and that the
            # data has actually persisted to disk!
            self.select_one('--commit--')
        else:
            # otherwise, we fire and forget as usual.
            self.execute('--commit--')

    def close(self, force=False):
        if force:
            # If a SqliteDict is being killed or garbage-collected, then select_one()
            # could hang forever because run() might already have exited and therefore
            # can't process the request. Instead, push the close command to the requests
            # queue directly. If run() is still alive, it will exit gracefully. If not,
            # then there's nothing we can do anyway.
            self.reqs.put(('--close--', None, Queue(), None))
        else:
            # we abuse 'select' to "iter" over a "--close--" statement so that we
            # can confirm the completion of close before joining the thread and
            # returning (by semaphore '--no more--'
            self.select_one('--close--')
            self.join()

    def _wait_for_initialization(self):
        """
        Polls the 'initialized' flag to be set by the started Thread in run().
        """
        # A race condition may occur without waiting for initialization:
        # __init__() finishes with the start() call, but the Thread needs some time to actually start working.
        # If opening the database file fails in run(), an exception will occur and self.exception will be set.
        # But if we run check_raise_error() before run() had a chance to set self.exception, it will report
        # a false negative: An exception occured and the thread terminates but self.exception is unset.
        # This leads to a deadlock while waiting for the results of execute().
        # By waiting for the Thread to set the initialized flag, we can ensure the thread has successfully
        # opened the file - and possibly set self.exception to be detected by check_raise_error().
        import time
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            if self._sqlitedict_thread_initialized or self.exception:
                return
            time.sleep(0.1)
        raise TimeoutError(
            f"SqliteMultithread failed to flag initialization"
            + f"withing {self.timeout:2f} seconds."
        )

import contextlib
import os
import tempfile
from unittest import TestCase

from app import main
from modules.dolt_api import execute_shell

pwd = os.getcwd()


@contextlib.contextmanager
def change_dir(path):
    print(f"change directory to {path}")
    os.chdir(path)
    yield
    os.chdir(pwd)


class TestDoltPull(TestCase):

    def test_sunny_workflow(self):
        main_path = os.path.join(os.path.abspath(os.path.dirname(main.__file__)), "main.py")
        rc, std, err = execute_shell("python", main_path, "dolt")
        print(main_path, rc, std, err)

        self.assertEqual(0, rc)

        with tempfile.TemporaryDirectory() as tmp:
            with change_dir(tmp):
                # dolt branch out
                rc, std, err = execute_shell(
                    "python", main_path, "dolt", "branchout", "-d", "adagrad/integration_test", "--force-init", "-s", "schema", "-b", "test_branch")
                self.assertEqual(0, rc, err)

                # add data
                rc, std, err = execute_shell(
                    "dolt", "sql", "-q", "insert into test values((select coalesce(max(pk) + 1, 0) from test), 'dasdsa')")
                self.assertEqual(0, rc, err)

                # dolt push and stage-all
                rc, std, err = execute_shell("python", main_path, "dolt", "push", "-a")
                self.assertEqual(0, rc, err)

        with tempfile.TemporaryDirectory() as tmp:
            with change_dir(tmp):
                # merge changes and resolve conflicts then push the result
                rc, std, err = execute_shell(
                    "python", main_path, "dolt", "merge", "-d", "adagrad/integration_test", "--force-clone",
                    "-s", "test_branch", "-t", "main", "-m", "a fancy commit message", "-p", "--delete-source"
                )
                self.assertEqual(0, rc, err)


    def test_conflicting_workflow(self):
        pass

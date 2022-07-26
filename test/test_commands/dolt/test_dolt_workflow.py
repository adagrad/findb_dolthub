import contextlib
import datetime
import os
import tempfile
from unittest import TestCase

from app import main
from modules.dolt_api import execute_shell, dolt_checkout_remote_branch

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
                sql = f"insert into test values({int(datetime.datetime.utcnow().timestamp())}, 'dasdsa')"
                rc, std, err = execute_shell("dolt", "sql", "-q", sql)
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
        main_path = os.path.join(os.path.abspath(os.path.dirname(main.__file__)), "main.py")
        rc, std, err = execute_shell("python", main_path, "dolt")
        self.assertEqual(0, rc)

        with tempfile.TemporaryDirectory() as tmp:
            with change_dir(tmp):
                tst = int(datetime.datetime.utcnow().timestamp())

                # dolt pull main
                rc, std, err = execute_shell(
                    "python", main_path, "dolt", "pull", "-d", "adagrad/integration_test", "--force-clone", "-b", "main")
                self.assertEqual(0, rc, err)

                # add data to main
                rc, std, err = execute_shell("dolt", "sql", "-q", f"insert into test values({tst}, 'dasdsa')")
                self.assertEqual(0, rc, err)
                rc, std, err = execute_shell("dolt", "add", ".")
                self.assertEqual(0, rc, err)
                rc, std, err = execute_shell("dolt", "commit", "-m", "test case")
                self.assertEqual(0, rc, err)

                # dolt branch out test branch
                rc, std, err = execute_shell(
                    "python", main_path, "dolt", "branchout", "-d", "adagrad/integration_test", "--force-init", "-s", "schema", "-b", "test_branch")
                self.assertEqual(0, rc, err)

                # add same data to test branch
                rc, std, err = execute_shell("dolt", "sql", "-q", f"insert into test values({tst}, 'dasdsa22')")
                self.assertEqual(0, rc, err)
                rc, std, err = execute_shell("dolt", "add", ".")
                self.assertEqual(0, rc, err)
                rc, std, err = execute_shell("dolt", "commit", "-m", "test case")
                self.assertEqual(0, rc, err)

                # merge changes and resolve conflicts then push the result
                rc, std, err = execute_shell(
                    "python", main_path, "dolt", "merge", "-d", "adagrad/integration_test",
                    "-s", "test_branch", "-t", "main", "-m", "a fancy commit message", "--theirs", "-p"
                )
                self.assertEqual(0, rc, err)


    def test_dolt_server(self):
        main_path = os.path.join(os.path.abspath(os.path.dirname(main.__file__)), "main.py")
        rc, std, err = execute_shell("python", main_path, "dolt")
        self.assertEqual(0, rc)

        with tempfile.TemporaryDirectory() as tmp:
            with change_dir(tmp):
                dolt_checkout_remote_branch("adagrad/integration_test", False, True, "main")

                rc, std, err = execute_shell(
                    "python", main_path, "dolt", "sqlserver", "-d", "adagrad/integration_test", "-b", "main",
                    "--and-exec", "dolt sql -q \"select count(*) from test;\""
                )
                print("\n", std, "\n", err)
                self.assertEqual(0, rc, err)

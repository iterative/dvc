import os
import posixpath
from hashlib import md5
from itertools import product

import pytest
from fsspec.implementations.local import LocalFileSystem, make_path_posix
from fsspec.tests.abstract.common import GLOB_EDGE_CASES_TESTS

from dvc.api import DVCFileSystem


class DVCFixtures:
    """The fixtures imitate the fsspec.tests.abstract.AbstractFixtures.

    This has been modified to use dvc's fixture, as DVCFileSystem is a read-only
    filesystem, and cannot be used to create directories or files.

    The `Output.ignore()` is mocked to avoid `.gitignore` files in the directories,
    as we can reuse the tests from fsspec with minimal modifications.
    `.gitignore` file is manually created with required patterns at the root of the
    repository.
    """

    @pytest.fixture
    def fs_bulk_operations_scenario_0(self, tmp_dir):
        """
        Scenario that is used for many cp/get/put tests. Creates the following
        directory and file structure:

        📁 source
        ├── 📄 file1
        ├── 📄 file2
        └── 📁 subdir
            ├── 📄 subfile1
            ├── 📄 subfile2
            └── 📁 nesteddir
                └── 📄 nestedfile
        """
        source = tmp_dir / "source"
        source.mkdir()
        tmp_dir.scm_gen(
            ".gitignore", "/source/file2/\nsource/subdir", commit="add .gitignore"
        )

        tmp_dir.scm_gen("source/file1", "file1", commit="add file1")
        tmp_dir.dvc_gen("source/file2", "file2", commit="add file2")
        tmp_dir.dvc_gen(
            {
                "source/subdir": {
                    "subfile1": "subfile1",
                    "subfile2": "subfile2",
                    "nesteddir": {"nestedfile": "nestedfile"},
                }
            },
            commit="add subdir",
        )
        return "/source"

    @pytest.fixture
    def fs_10_files_with_hashed_names(self, tmp_dir, local_fs, local_join, local_path):
        """
        Scenario that is used to check cp/get/put files order when source and
        destination are lists. Creates the following directory and file structure:

        📁 source
        └── 📄 {hashed([0-9])}.txt
        """
        dir_contents = {
            md5(str(i).encode("utf-8"), usedforsecurity=False).hexdigest()
            + ".txt": str(i)
            for i in range(10)
        }
        tmp_dir.dvc_gen({"source": dir_contents}, commit="add source")
        tmp_dir.scm_gen(".gitignore", "/source", commit="add .gitignore")
        return "/source"

    @pytest.fixture
    def src_directory(self, tmp_dir):
        # https://github.com/fsspec/filesystem_spec/issues/1062
        # Recursive cp/get/put of source directory into non-existent target directory.
        tmp_dir.dvc_gen({"src": {"file": "file"}}, commit="add source")
        return "/src"

    @pytest.fixture
    def fs_dir_and_file_with_same_name_prefix(self, tmp_dir):
        """
        Scenario that is used to check cp/get/put on directory and file with
        the same name prefixes. Creates the following directory and file structure:

        📁 source
        ├── 📄 subdir.txt
        └── 📁 subdir
            └── 📄 subfile.txt
        """
        source = tmp_dir / "source"
        source.mkdir()

        tmp_dir.scm_gen(".gitignore", "/source/subdir", commit="add .gitignore")
        tmp_dir.scm_gen("source/subdir.txt", "subdir.txt", commit="add subdir.txt")
        tmp_dir.dvc_gen(
            {"source/subdir": {"subfile.txt": "subfile.txt"}}, commit="add subdir"
        )
        return "/source"

    @pytest.fixture
    def fs_glob_edge_cases_files(self, tmp_dir):
        """
        Scenario that is used for glob edge cases cp/get/put tests.
        Creates the following directory and file structure:

        📁 source
        ├── 📄 file1
        ├── 📄 file2
        ├── 📁 subdir0
        │   ├── 📄 subfile1
        │   ├── 📄 subfile2
        │   └── 📁 nesteddir
        │       └── 📄 nestedfile
        └── 📁 subdir1
            ├── 📄 subfile1
            ├── 📄 subfile2
            └── 📁 nesteddir
                └── 📄 nestedfile
        """
        source = tmp_dir / "source"
        source.mkdir()

        tmp_dir.scm_gen(
            ".gitignore", "/source/file1\n/source/subdir1", commit="add .gitignore"
        )
        tmp_dir.scm_gen("source/file1", "file1", commit="add file1")
        tmp_dir.dvc_gen("source/file2", "file2", commit="add file2")

        dir_contents = {
            "subfile1": "subfile1",
            "subfile2": "subfile2",
            "nesteddir": {"nestedfile": "nestedfile"},
        }
        tmp_dir.scm_gen({"source/subdir0": dir_contents}, commit="add subdir0")
        tmp_dir.dvc_gen({"source/subdir1": dir_contents}, commit="add subdir1")
        return "/source"

    @pytest.fixture(params=[{"rev": "HEAD"}, {}])
    def fs(self, request, tmp_dir, dvc, scm):
        return DVCFileSystem(tmp_dir, **request.param)

    @pytest.fixture(autouse=True)
    def mock_ignore(self, mocker):
        mocker.patch("dvc.output.Output.ignore")

    @pytest.fixture
    def fs_join(self):
        return posixpath.join

    @pytest.fixture
    def fs_path(self, fs):
        return fs.root_marker

    @pytest.fixture(scope="class")
    def local_fs(self):
        # Maybe need an option for auto_mkdir=False?  This is only relevant
        # for certain implementations.
        return LocalFileSystem(auto_mkdir=True)

    @pytest.fixture
    def local_join(self):
        """
        Return a function that joins its arguments together into a path, on
        the local filesystem.
        """
        return os.path.join

    @pytest.fixture
    def local_path(self, tmpdir):
        return tmpdir

    @pytest.fixture
    def local_target(self, local_fs, local_join, local_path):
        """
        Return name of local directory that does not yet exist to copy into.

        Cleans up at the end of each test it which it is used.
        """
        target = local_join(local_path, "target")
        yield target
        if local_fs.exists(target):
            local_fs.rm(target, recursive=True)


class TestDVCFileSystemGet(DVCFixtures):
    """
    This test is adapted from `fsspec.tests.abstract.get.AbstractGetTests`
    with minor modifications to work with DVCFixtures and DVCFileSystem.
    """

    def test_get_file_to_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1a
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)
        assert local_fs.isdir(target)

        target_file2 = local_join(target, "file2")
        target_subfile1 = local_join(target, "subfile1")

        # Copy from source directory
        fs.get(fs_join(source, "file2"), target)
        assert local_fs.isfile(target_file2)

        # Copy from sub directory
        fs.get(fs_join(source, "subdir", "subfile1"), target)
        assert local_fs.isfile(target_subfile1)

        # Remove copied files
        local_fs.rm([target_file2, target_subfile1])
        assert not local_fs.exists(target_file2)
        assert not local_fs.exists(target_subfile1)

        # Repeat with trailing slash on target
        fs.get(fs_join(source, "file2"), target + "/")
        assert local_fs.isdir(target)
        assert local_fs.isfile(target_file2)

        fs.get(fs_join(source, "subdir", "subfile1"), target + "/")
        assert local_fs.isfile(target_subfile1)

    def test_get_file_to_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1b
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        fs.get(
            fs_join(source, "subdir", "subfile1"), local_join(target, "newdir/")
        )  # Note trailing slash

        assert local_fs.isdir(target)
        assert local_fs.isdir(local_join(target, "newdir"))
        assert local_fs.isfile(local_join(target, "newdir", "subfile1"))

    def test_get_file_to_file_in_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1c
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        fs.get(fs_join(source, "subdir", "subfile1"), local_join(target, "newfile"))
        assert local_fs.isfile(local_join(target, "newfile"))

    def test_get_file_to_file_in_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1d
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        fs.get(
            fs_join(source, "subdir", "subfile1"),
            local_join(target, "newdir", "newfile"),
        )
        assert local_fs.isdir(local_join(target, "newdir"))
        assert local_fs.isfile(local_join(target, "newdir", "newfile"))

    def test_get_directory_to_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1e
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)
        assert local_fs.isdir(target)

        for source_slash, target_slash in zip([False, True], [False, True]):
            s = fs_join(source, "subdir")
            if source_slash:
                s += "/"
            t = target + "/" if target_slash else target

            # Without recursive does nothing
            fs.get(s, t)
            assert local_fs.ls(target) == []

            # With recursive
            fs.get(s, t, recursive=True)
            if source_slash:
                assert local_fs.isfile(local_join(target, "subfile1"))
                assert local_fs.isfile(local_join(target, "subfile2"))
                assert local_fs.isdir(local_join(target, "nesteddir"))
                assert local_fs.isfile(local_join(target, "nesteddir", "nestedfile"))
                assert not local_fs.exists(local_join(target, "subdir"))

                local_fs.rm(
                    [
                        local_join(target, "subfile1"),
                        local_join(target, "subfile2"),
                        local_join(target, "nesteddir"),
                    ],
                    recursive=True,
                )
            else:
                assert local_fs.isdir(local_join(target, "subdir"))
                assert local_fs.isfile(local_join(target, "subdir", "subfile1"))
                assert local_fs.isfile(local_join(target, "subdir", "subfile2"))
                assert local_fs.isdir(local_join(target, "subdir", "nesteddir"))
                assert local_fs.isfile(
                    local_join(target, "subdir", "nesteddir", "nestedfile")
                )

                local_fs.rm(local_join(target, "subdir"), recursive=True)
            assert local_fs.ls(target) == []

            # Limit recursive by maxdepth
            fs.get(s, t, recursive=True, maxdepth=1)
            if source_slash:
                assert local_fs.isfile(local_join(target, "subfile1"))
                assert local_fs.isfile(local_join(target, "subfile2"))
                assert not local_fs.exists(local_join(target, "nesteddir"))
                assert not local_fs.exists(local_join(target, "subdir"))

                local_fs.rm(
                    [
                        local_join(target, "subfile1"),
                        local_join(target, "subfile2"),
                    ],
                    recursive=True,
                )
            else:
                assert local_fs.isdir(local_join(target, "subdir"))
                assert local_fs.isfile(local_join(target, "subdir", "subfile1"))
                assert local_fs.isfile(local_join(target, "subdir", "subfile2"))
                assert not local_fs.exists(local_join(target, "subdir", "nesteddir"))

                local_fs.rm(local_join(target, "subdir"), recursive=True)
            assert local_fs.ls(target) == []

    def test_get_directory_to_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1f
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        for source_slash, target_slash in zip([False, True], [False, True]):
            s = fs_join(source, "subdir")
            if source_slash:
                s += "/"
            t = local_join(target, "newdir")
            if target_slash:
                t += "/"

            # Without recursive does nothing
            fs.get(s, t)
            assert local_fs.ls(target) == []

            # With recursive
            fs.get(s, t, recursive=True)
            assert local_fs.isdir(local_join(target, "newdir"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile1"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile2"))
            assert local_fs.isdir(local_join(target, "newdir", "nesteddir"))
            assert local_fs.isfile(
                local_join(target, "newdir", "nesteddir", "nestedfile")
            )
            assert not local_fs.exists(local_join(target, "subdir"))

            local_fs.rm(local_join(target, "newdir"), recursive=True)
            assert local_fs.ls(target) == []

            # Limit recursive by maxdepth
            fs.get(s, t, recursive=True, maxdepth=1)
            assert local_fs.isdir(local_join(target, "newdir"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile1"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile2"))
            assert not local_fs.exists(local_join(target, "newdir", "nesteddir"))
            assert not local_fs.exists(local_join(target, "subdir"))

            local_fs.rm(local_join(target, "newdir"), recursive=True)
            assert not local_fs.exists(local_join(target, "newdir"))

    def test_get_glob_to_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1g
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        for target_slash in [False, True]:
            t = target + "/" if target_slash else target

            # Without recursive
            fs.get(fs_join(source, "subdir", "*"), t)
            assert local_fs.isfile(local_join(target, "subfile1"))
            assert local_fs.isfile(local_join(target, "subfile2"))
            assert not local_fs.isdir(local_join(target, "nesteddir"))
            assert not local_fs.exists(local_join(target, "nesteddir", "nestedfile"))
            assert not local_fs.exists(local_join(target, "subdir"))

            local_fs.rm(
                [
                    local_join(target, "subfile1"),
                    local_join(target, "subfile2"),
                ],
                recursive=True,
            )
            assert local_fs.ls(target) == []

            # With recursive
            for glob, recursive in zip(["*", "**"], [True, False]):
                fs.get(fs_join(source, "subdir", glob), t, recursive=recursive)
                assert local_fs.isfile(local_join(target, "subfile1"))
                assert local_fs.isfile(local_join(target, "subfile2"))
                assert local_fs.isdir(local_join(target, "nesteddir"))
                assert local_fs.isfile(local_join(target, "nesteddir", "nestedfile"))
                assert not local_fs.exists(local_join(target, "subdir"))

                local_fs.rm(
                    [
                        local_join(target, "subfile1"),
                        local_join(target, "subfile2"),
                        local_join(target, "nesteddir"),
                    ],
                    recursive=True,
                )
                assert local_fs.ls(target) == []

                # Limit recursive by maxdepth
                fs.get(
                    fs_join(source, "subdir", glob), t, recursive=recursive, maxdepth=1
                )
                assert local_fs.isfile(local_join(target, "subfile1"))
                assert local_fs.isfile(local_join(target, "subfile2"))
                assert not local_fs.exists(local_join(target, "nesteddir"))
                assert not local_fs.exists(local_join(target, "subdir"))

                local_fs.rm(
                    [
                        local_join(target, "subfile1"),
                        local_join(target, "subfile2"),
                    ],
                    recursive=True,
                )
                assert local_fs.ls(target) == []

    def test_get_glob_to_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1h
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        for target_slash in [False, True]:
            t = fs_join(target, "newdir")
            if target_slash:
                t += "/"

            # Without recursive
            fs.get(fs_join(source, "subdir", "*"), t)
            assert local_fs.isdir(local_join(target, "newdir"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile1"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile2"))
            assert not local_fs.exists(local_join(target, "newdir", "nesteddir"))
            assert not local_fs.exists(
                local_join(target, "newdir", "nesteddir", "nestedfile")
            )
            assert not local_fs.exists(local_join(target, "subdir"))
            assert not local_fs.exists(local_join(target, "newdir", "subdir"))

            local_fs.rm(local_join(target, "newdir"), recursive=True)
            assert local_fs.ls(target) == []

            # With recursive
            for glob, recursive in zip(["*", "**"], [True, False]):
                fs.get(fs_join(source, "subdir", glob), t, recursive=recursive)
                assert local_fs.isdir(local_join(target, "newdir"))
                assert local_fs.isfile(local_join(target, "newdir", "subfile1"))
                assert local_fs.isfile(local_join(target, "newdir", "subfile2"))
                assert local_fs.isdir(local_join(target, "newdir", "nesteddir"))
                assert local_fs.isfile(
                    local_join(target, "newdir", "nesteddir", "nestedfile")
                )
                assert not local_fs.exists(local_join(target, "subdir"))
                assert not local_fs.exists(local_join(target, "newdir", "subdir"))

                local_fs.rm(local_join(target, "newdir"), recursive=True)
                assert not local_fs.exists(local_join(target, "newdir"))

                # Limit recursive by maxdepth
                fs.get(
                    fs_join(source, "subdir", glob), t, recursive=recursive, maxdepth=1
                )
                assert local_fs.isdir(local_join(target, "newdir"))
                assert local_fs.isfile(local_join(target, "newdir", "subfile1"))
                assert local_fs.isfile(local_join(target, "newdir", "subfile2"))
                assert not local_fs.exists(local_join(target, "newdir", "nesteddir"))
                assert not local_fs.exists(local_join(target, "subdir"))
                assert not local_fs.exists(local_join(target, "newdir", "subdir"))

                local_fs.rm(local_fs.ls(target, detail=False), recursive=True)
                assert not local_fs.exists(local_join(target, "newdir"))

    @pytest.mark.parametrize(
        GLOB_EDGE_CASES_TESTS["argnames"],
        GLOB_EDGE_CASES_TESTS["argvalues"],
    )
    def test_get_glob_edge_cases(
        self,
        path,
        recursive,
        maxdepth,
        expected,
        fs,
        fs_join,
        fs_glob_edge_cases_files,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1g
        source = fs_glob_edge_cases_files

        target = local_target

        for new_dir, target_slash in product([True, False], [True, False]):
            local_fs.mkdir(target)

            t = local_join(target, "newdir") if new_dir else target
            t = t + "/" if target_slash else t

            fs.get(fs_join(source, path), t, recursive=recursive, maxdepth=maxdepth)

            output = local_fs.find(target)
            if new_dir:
                prefixed_expected = [
                    make_path_posix(local_join(target, "newdir", p)) for p in expected
                ]
            else:
                prefixed_expected = [
                    make_path_posix(local_join(target, p)) for p in expected
                ]
            assert sorted(output) == sorted(prefixed_expected)

            try:
                local_fs.rm(target, recursive=True)
            except FileNotFoundError:
                pass

    def test_get_list_of_files_to_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 2a
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        source_files = [
            fs_join(source, "file1"),
            fs_join(source, "file2"),
            fs_join(source, "subdir", "subfile1"),
        ]

        for target_slash in [False, True]:
            t = target + "/" if target_slash else target

            fs.get(source_files, t)
            assert local_fs.isfile(local_join(target, "file1"))
            assert local_fs.isfile(local_join(target, "file2"))
            assert local_fs.isfile(local_join(target, "subfile1"))

            local_fs.rm(
                [
                    local_join(target, "file1"),
                    local_join(target, "file2"),
                    local_join(target, "subfile1"),
                ],
                recursive=True,
            )
            assert local_fs.ls(target) == []

    def test_get_list_of_files_to_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 2b
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        source_files = [
            fs_join(source, "file1"),
            fs_join(source, "file2"),
            fs_join(source, "subdir", "subfile1"),
        ]

        fs.get(source_files, local_join(target, "newdir") + "/")  # Note trailing slash
        assert local_fs.isdir(local_join(target, "newdir"))
        assert local_fs.isfile(local_join(target, "newdir", "file1"))
        assert local_fs.isfile(local_join(target, "newdir", "file2"))
        assert local_fs.isfile(local_join(target, "newdir", "subfile1"))

    def test_get_directory_recursive(
        self, src_directory, fs, fs_join, fs_path, local_fs, local_join, local_target
    ):
        target = local_target
        src = src_directory

        # get without slash
        assert not local_fs.exists(target)
        for loop in range(2):
            fs.get(src, target, recursive=True)
            assert local_fs.isdir(target)

            if loop == 0:
                assert local_fs.isfile(local_join(target, "file"))
                assert not local_fs.exists(local_join(target, "src"))
            else:
                assert local_fs.isfile(local_join(target, "file"))
                assert local_fs.isdir(local_join(target, "src"))
                assert local_fs.isfile(local_join(target, "src", "file"))

        local_fs.rm(target, recursive=True)

        # get with slash
        assert not local_fs.exists(target)
        for _ in range(2):
            fs.get(src + "/", target, recursive=True)
            assert local_fs.isdir(target)
            assert local_fs.isfile(local_join(target, "file"))
            assert not local_fs.exists(local_join(target, "src"))

    def test_get_directory_without_files_with_same_name_prefix(
        self,
        fs,
        fs_join,
        local_fs,
        local_join,
        local_target,
        fs_dir_and_file_with_same_name_prefix,
    ):
        # Create the test dirs
        source = fs_dir_and_file_with_same_name_prefix
        target = local_target

        # Test without glob
        fs.get(fs_join(source, "subdir"), target, recursive=True)

        assert local_fs.isfile(local_join(target, "subfile.txt"))
        assert not local_fs.isfile(local_join(target, "subdir.txt"))

        local_fs.rm([local_join(target, "subfile.txt")])
        assert local_fs.ls(target) == []

        # Test with glob
        fs.get(fs_join(source, "subdir*"), target, recursive=True)

        assert local_fs.isdir(local_join(target, "subdir"))
        assert local_fs.isfile(local_join(target, "subdir", "subfile.txt"))
        assert local_fs.isfile(local_join(target, "subdir.txt"))

    def test_get_with_source_and_destination_as_list(
        self,
        fs,
        fs_join,
        local_fs,
        local_join,
        local_target,
        fs_10_files_with_hashed_names,
    ):
        # Create the test dir
        source = fs_10_files_with_hashed_names
        target = local_target

        # Create list of files for source and destination
        source_files = []
        destination_files = []
        for i in range(10):
            hashed_i = md5(str(i).encode("utf-8"), usedforsecurity=False).hexdigest()
            source_files.append(fs_join(source, f"{hashed_i}.txt"))
            destination_files.append(
                make_path_posix(local_join(target, f"{hashed_i}.txt"))
            )

        # Copy and assert order was kept
        fs.get(rpath=source_files, lpath=destination_files)

        for i in range(10):
            file_content = local_fs.cat(destination_files[i]).decode("utf-8")
            assert file_content == str(i)


def test_maxdepth(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen(
        {
            "dir": {
                "file1": "file1",
                "subdir": {
                    "file2": "file2",
                    "subdir2": {"file3": "file3", "subdir3": {"file4": "file4"}},
                },
            }
        },
        commit="add dir",
    )

    fs = DVCFileSystem(tmp_dir)
    fs.get("dir", "dir1", recursive=True, maxdepth=1)
    assert (tmp_dir / "dir1").read_text() == {"file1": "file1"}

    fs.get("dir", "dir2", recursive=True, maxdepth=2)
    assert (tmp_dir / "dir2").read_text() == {
        "file1": "file1",
        "subdir": {"file2": "file2"},
    }

    fs.get("dir", "dir3", recursive=True, maxdepth=3)
    assert (tmp_dir / "dir3").read_text() == {
        "file1": "file1",
        "subdir": {"file2": "file2", "subdir2": {"file3": "file3"}},
    }

    fs.get("dir", "dir4", recursive=True, maxdepth=4)
    assert (tmp_dir / "dir4").read_text() == {
        "file1": "file1",
        "subdir": {
            "file2": "file2",
            "subdir2": {"file3": "file3", "subdir3": {"file4": "file4"}},
        },
    }


@pytest.mark.parametrize(
    "fs_args",
    [
        lambda tmp_dir, dvc: ((), {}),  # noqa: ARG005
        lambda tmp_dir, dvc: ((dvc,), {}),  # noqa: ARG005
        lambda tmp_dir, dvc: ((tmp_dir,), {}),  # noqa: ARG005
        lambda tmp_dir, dvc: ((str(tmp_dir),), {}),  # noqa: ARG005
        lambda tmp_dir, dvc: ((), {"repo": tmp_dir}),  # noqa: ARG005
        lambda tmp_dir, dvc: ((), {"repo": os.fspath(tmp_dir)}),  # noqa: ARG005
        # url= is deprecated, but is still supported for backward compatibility
        lambda tmp_dir, dvc: ((), {"url": tmp_dir}),  # noqa: ARG005
        lambda tmp_dir, dvc: ((), {"url": os.fspath(tmp_dir)}),  # noqa: ARG005
        lambda tmp_dir, dvc: ((), {"repo": dvc}),  # noqa: ARG005
    ],
)
def test_init_arg(tmp_dir, dvc, fs_args):
    args, kwargs = fs_args(tmp_dir, dvc)
    fs = DVCFileSystem(*args, **kwargs)

    assert fs.repo.root_dir == dvc.root_dir

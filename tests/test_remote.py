from dvc.main import main
from dvc.command.remote import CmdRemoteAdd

from tests.basic_env import TestDvc


class TestRemote(TestDvc):
    def test(self):
        remotes = ['a', 'b', 'c']

        self.assertEqual(main(['remote', 'list']), 0)
        self.assertNotEqual(main(['remote', 'remove', remotes[0]]), 0)

        for r in remotes:
            self.assertEqual(main(['remote', 'add', '--default', r, 's3://bucket/name']), 0)

        self.assertEqual(main(['remote', 'list']), 0)

        self.assertEqual(main(['remote', 'remove', remotes[0]]), 0)
        self.assertEqual(main(['remote', 'modify', remotes[0], 'option', 'value']), 0)

        self.assertEqual(main(['remote', 'list']), 0)

    def test_failed_write(self):   
        class A(object):           
            local = False          
            name = 'myremote'   
            url = 's3://remote'     
            unset = False          
                                   
        args = A()                 
        cmd = CmdRemoteAdd(args)      
                                   
        cmd.configobj.write = None 
        ret = cmd.run()           
        self.assertNotEqual(ret, 0)

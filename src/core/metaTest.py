import unittest
import random
import db.connection

class TestMeta(unittest.TestCase):

  def setUp(self):
    self.c = db.connection.Connection('dbv','dbv')
        
  def test_meta(self):
    self.assertTrue(self.c)
    self.assertEqual(None, self.c.current_repo)
    self.c.create_repo('test')
    print "Set repo to test"
    self.c.set_current_repo('test')
    print self.c.g
    print ''
    print "Create fork t1"
    self.c.create_fork('master','t1')
    print self.c.g
    self.assertTrue('master',self.c.get_current_branch)
    self.c.set_current_branch('t1')
    self.assertTrue('t1',self.c.get_current_branch)
    
    print "\ntest read\n"
    print self.c.test_read()
    
    print '\nInsert\n'
    self.c.test_insert(1,'aaron','il',30)
    print self.c.g
    print "\ntest read"
    print self.c.test_read()
    
    print '\nCreate fork t2\n'
    self.c.create_fork('t1','t2')
    self.c.set_current_branch('t2')
    
    print '\nInsert\n'
    self.c.test_insert(2,'sam','ma',30)
    print self.c.g
    print "\ntest read"
    print self.c.test_read()
    
    
if __name__ == '__main__':
  unittest.main()
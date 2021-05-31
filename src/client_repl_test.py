import unittest

from client_repl import *

class TestParser(unittest.TestCase):

    def test_one(self):
        self.assertEqual(parse_line("publish hi message"), Publish("hi", "message"))
    
    def test_two(self):
        self.assertEqual(parse_line("consume hi"), Consume("hi", None))

    def test_three(self):
        self.assertEqual(parse_line("consume hi 3"), Consume("hi", 3))
    
    def test_four(self):
        self.assertEqual(parse_line("q"), Quit())
        
    def test_five(self):
        self.assertEqual(parse_line("primary test_key"), Primary("test_key"))

    def test_six(self):
        self.assertEqual(parse_line("brokers test_key"), Brokers())

    def test_seven(self):
        self.assertEqual(parse_line("target localhost:3001"), Target("localhost:3001"))

    def test_8(self):
        self.assertEqual(parse_args("\"first one\" \"second one\""), ["first one", "second one"])
    
    def test_9(self):
        self.assertEqual(parse_args("first one \"second one\""), ["first", "one", "second one"])

    def test_10(self):
        self.assertEqual(parse_args("first \"one second\" one"), ["first", "one second", "one"])

if __name__ == '__main__':
    unittest.main()
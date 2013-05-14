import unittest
from nutch2.runner.nutch import NutchCommandGenerator


class CommandGeneratorTests(unittest.TestCase):
    def setUp(self):
        self.cg = NutchCommandGenerator()

    def test_inject(self):
        expected = './nutch inject urls'
        actual = self.cg.inject_seed_dir("urls")
        self.assertEqual(expected, actual)

    def test_inject_crawlid(self):
        expected = './nutch inject urls -crawlId 13'
        actual = self.cg.inject_seed_dir("urls", crawlId=13)
        self.assertEqual(expected, actual)

    def test_generate(self):
        #  GeneratorJob [-topN N] [-crawlId id] [-noFilter] [-noNorm] [-adddays numDays]
        expected = './nutch generate'
        actual = self.cg.generate()
        self.assertEqual(expected, actual)

    def test_generate_complex(self):
        #  GeneratorJob [-topN N] [-crawlId id] [-noFilter] [-noNorm] [-adddays numDays]
        expected = './nutch generate -crawlId 4 -noFilter true -topN 3'
        actual = self.cg.generate(topN=3, crawlId=4, noFilter="true")
        self.assertEqual(expected, actual)

    def test_fetch(self):
        # Usage: FetcherJob (<batchId> | -all) [-crawlId <id>] [-threads N] [-resume] [-numTasks N]
        expected = './nutch fetch 35'
        actual = self.cg.fetch(35)
        self.assertEqual(expected, actual)

    def test_fetch_all(self):
        expected = './nutch fetch -all'
        actual = self.cg.fetch("-all")
        self.assertEqual(expected, actual)

    def test_fetch_complex(self):
        expected = './nutch fetch 35 -crawlId 4 -threads 3'
        actual = self.cg.fetch(35, crawlId=4, threads=3)
        self.assertEqual(expected, actual)

    def test_parser(self):
        expected = './nutch parse 12'
        actual = self.cg.parse(12)
        self.assertEqual(expected, actual)

    def test_parser_crawlid(self):
        expected = './nutch parse 35 -crawlId 4'
        actual = self.cg.parse(35, crawlId=4)
        self.assertEqual(expected, actual)

    def test_parser_resume(self):
        #Usage: ParserJob (<batchId> | -all) [-crawlId <id>] [-resume] [-force]
        expected = './nutch parse 35 -crawlId 4 -resume'
        actual = self.cg.parse(35, resume=True, crawlId=4)
        self.assertEqual(expected, actual)

    def test_parser_not_resume(self):
        #Usage: ParserJob (<batchId> | -all) [-crawlId <id>] [-resume] [-force]
        expected = './nutch parse 35 -crawlId 4'
        actual = self.cg.parse(35, resume=False, crawlId=4)
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
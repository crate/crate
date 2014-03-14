import unittest
import doctest
import zc.customdoctests
from crate.testing.layer import CrateLayer
import os
import requests
import shutil
import re
import json

from crate.crash.command import CrateCmd
cmd = CrateCmd()


here = os.path.dirname(__file__)


def project_path(*parts):
    return os.path.join(os.path.dirname(docs_path()), *parts)


def docs_path(*parts):
    return os.path.join(os.path.dirname(os.path.dirname(here)), *parts)


def crate_path(*parts):
    return docs_path('tmp', 'crate', *parts)


def bash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    s = s.replace(':4200/', ':44200/')
    if s.startswith("crash"):
        s = re.search(r"crash\s+-c\s+\"(.*?)\"", s).group(1)
        return ('cmd.onecmd("""{0}""");'.format(s.strip()))
    return (
        r'import subprocess;'
        r'print(subprocess.check_output(r"""%s""",stderr=subprocess.STDOUT,shell=True))' % s) + '\n'


def crash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    s = s.replace(':4200', ':44200')
    return ('cmd.onecmd("""{0}""");'.format(s.strip()))


bash_parser = zc.customdoctests.DocTestParser(
    ps1='sh\$', comment_prefix='#', transform=bash_transform)

crash_parser = zc.customdoctests.DocTestParser(
    ps1='cr>', comment_prefix='#', transform=crash_transform)


class ConnectingCrateLayer(CrateLayer):

    def start(self):
        super(ConnectingCrateLayer, self).start()
        cmd.do_connect(self.crate_servers[0])

empty_layer = ConnectingCrateLayer('crate',
                         crate_home=crate_path(),
                         crate_exec=crate_path('bin', 'crate'),
                         port=44200,
                         transport_port=44300)

def setUpLocations(test):
    test.globs['cmd'] = cmd

    cmd.onecmd("""
        create table locations (
          id string primary key,
          name string,
          date timestamp,
          kind string,
          position integer,
          description string,
          race object(dynamic) as (
            interests array(string)
          ),
          index name_description_ft using fulltext(name, description) with (analyzer='english')
        ) clustered by(id) into 2 shards with (number_of_replicas=0)""".strip())
    cmd.onecmd("delete from locations")
    rows = [
        { "id": "1", "name": "North West Ripple", "date":308534400000, "kind": "Galaxy", "position": 1, "description": "Relative to life on NowWhat, living on an affluent world in the North West ripple of the Galaxy is said to be easier by a factor of about seventeen million."},
        { "id": "2", "name": "Outer Eastern Rim", "date":308534400000, "kind": "Galaxy", "position": 2, "description": "The Outer Eastern Rim of the Galaxy where the Guide has supplanted the Encyclopedia Galactica among its more relaxed civilisations."},
        { "id": "3", "name": "Galactic Sector QQ7 Active J Gamma", "date":"2013-05-01", "kind": "Galaxy", "position": 4, "description": "Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains."},
        { "id": "4", "name": "Aldebaran", "date":1373932800000, "kind": "Star System", "position": 1, "description": "Max Quordlepleen claims that the only thing left after the end of the Universe will be the sweets trolley and a fine selection of Aldebaran liqueurs."},
        { "id": "5", "name": "Algol", "date":1373932800000, "kind": "Star System", "position": 2, "description": "Algol is the home of the Algolian Suntiger, the tooth of which is one of the ingredients of the Pan Galactic Gargle Blaster."},
        { "id": "6", "name": "Alpha Centauri", "date":308534400000, "kind": "Star System", "position": 3, "description": "4.1 light-years northwest of earth"},
        { "id": "7", "name": "Altair", "date":1373932800000, "kind": "Star System", "position": 4, "description": "The Altairian dollar is one of three freely convertible currencies in the galaxy, though by the time of the novels it had apparently recently collapsed."},
        { "id": "8", "name": "Allosimanius Syneca", "date":"2013-07-16", "kind": "Planet", "position": 1, "description": "Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold."},
        { "id": "9", "name": "Argabuthon", "date":1373932800000, "kind": "Planet", "position": 2, "description": "It is also the home of Prak, a man placed into solitary confinement after an overdose of truth drug caused him to tell the Truth in its absolute and final form, causing anyone to hear it to go insane."},
        { "id": "10", "name": "Arkintoofle Minor", "date":308534400000, "kind": "Planet", "position": 3, "description": "Motivated by the fact that the only thing in the Universe that travels faster than light is bad news, the Hingefreel people native to Arkintoofle Minor constructed a starship driven by bad news."},
        { "id": "11", "name": "Bartledan", "date":1373932800000, "kind": "Planet", "position": 4, "description": "An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.", "race": {"name": "Bartledannians", "description": "Similar to humans, but do not breathe", "interests": ["netball"] } },
        { "id": "12", "name": "", "date":1373932800000, "kind": "Planet", "position": 5, "description": "This Planet doesn't really exist"},
        { "id": "13", "name": None, "date":1373932800000, "kind": "Galaxy", "position": 6, "description": "The end of the Galaxy.%"},
    ]
    for row in rows:
        stmt = "insert into locations (%s) values (%s)" % (
            ", ".join(row.keys()),
            ", ".join("?" for v in row.values())
        )
        args = row.values()
        response = requests.post('http://localhost:44200/_sql?refresh=true', json.dumps({
                "stmt": stmt,
                "args": args
            })
        )
        if response.status_code != 200:
            print response.status_code
            print response.content
    cmd.onecmd("""refresh table locations""")

def setUpQuotes(test):
    test.globs['cmd'] = cmd
    cmd.onecmd("""
        create table quotes (
          id integer primary key,
          quote string
        ) clustered by(id) into 2 shards with(number_of_replicas=0)
    """.strip())

    crate_wd = empty_layer.wdPath()
    cluster_name = "Testing44200"
    import_dir = os.path.join(crate_wd, cluster_name, "nodes", "0", "import_data")
    if not os.path.isdir(import_dir):
        os.mkdir(import_dir)
    shutil.copy(project_path('sql/src/test/resources/essetup/data/copy', 'test_copy_from.json'),
                os.path.join(import_dir, "quotes.json"))


def setUpLocationsAndQuotes(test):
    setUpLocations(test)
    setUpQuotes(test)

def setUp(test):
    test.globs['cmd'] = cmd

def tearDownDropQuotes(test):
    cmd.onecmd("drop table quotes")

def test_suite():
    suite = unittest.TestSuite()
    s = doctest.DocFileSuite('../../blob.txt',
                             parser=bash_parser,
                             setUp=setUp,
                             tearDown=tearDownDropQuotes,
                             optionflags=doctest.NORMALIZE_WHITESPACE |
                             doctest.ELLIPSIS)
    s.layer = empty_layer
    suite.addTest(s)
    for fn in ('sql/rest.txt',):
        s = doctest.DocFileSuite('../../' + fn,
                                 parser=bash_parser,
                                 setUp=setUpLocations,
                                 optionflags=doctest.NORMALIZE_WHITESPACE |
                                 doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    for fn in ('sql/dml.txt', 'sql/occ.txt', 'sql/ddl.txt',
               'sql/information_schema.txt', 'sql/aggregation.txt',
               'sql/scalar.txt', 'sql/stats.txt',
               'hello.txt'):
        s = doctest.DocFileSuite('../../' + fn, parser=crash_parser,
                                 setUp=setUpLocationsAndQuotes,
                                 optionflags=doctest.NORMALIZE_WHITESPACE |
                                 doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    return suite

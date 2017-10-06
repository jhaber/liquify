import argparse
import subprocess
import sys
import os

DEVNULL = open(os.devnull, 'r+b', 0)

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', dest='root', default=os.getcwd())
    args = p.parse_args()

    verify_directory(args.root)
    migrations = find_liquibase_migrations(args.root)

    pom_to_version_cache = {}
    for migration in migrations:
        print 'Found xml liquibase migration \'{0}\''.format(migration)

        dir, _ = os.path.split(migration)

        while not 'pom.xml' in os.listdir(dir) and dir != args.root:
            dir, _ = os.path.split(dir)

        if not 'pom.xml' in os.listdir(dir):
            print 'No pom.xml found in parent directories of \'{0}\''.format(migration)
            sys.exit(1)

        pom = os.path.join(dir, 'pom.xml')
        print 'Using pom \'{0}\' to discover liquibase version'.format(pom)

        if pom in pom_to_version_cache:
            version = pom_to_version_cache[pom]
        else:
            output = run_command('mvn dependency:list | grep -o -e \'org.liquibase:liquibase-core:jar:[.0-9]*\'', dir)[:-1]
            version = output.rsplit(':', 1)[1]
            pom_to_version_cache[pom] = version

        print 'Determined liquibase version to be {0}'.format(version)

        jar_name = 'liquify-{0}.jar'.format(version)
        jar_url = 'https://github.com/jhaber/liquify/releases/download/0.6/{0}'.format(jar_name)
        local_jar_path = os.path.join('/tmp', jar_name)

        if (os.path.isfile(local_jar_path)):
            print 'Using cached liquify jar \'{0}\''.format(local_jar_path)
        else:
            print 'Downloading liquify jar to \'{0}\''.format(local_jar_path)
            download = subprocess.Popen('curl {0} -o {1} --fail --location'.format(jar_url, local_jar_path), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output = download.communicate()[0]
            returncode = download.returncode
            if returncode != 0:
                print output
                print 'Unable to fetch jar from url \'{0}\''.format(jar_url)
                sys.exit(1)

        migration_dir, _ = os.path.split(migration)
        returncode = subprocess.call(['java', '-jar', local_jar_path, '-t', 'sql', '-db', 'mysql', migration], cwd=migration_dir)
        if returncode == 0:
            print 'Converted \'{0}\' to sql\n'.format(migration)
        else:
            print 'Error trying to convert \'{0}\' to sql'.format(migration)
            sys.exit(1)



def run_command(cmd, dir):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, cwd=dir).communicate()[0]


def find_liquibase_migrations(dir):
    migrations = []
    for root, _, files in os.walk(dir):
        possible_migrations = [os.path.join(root, file) for file in files if file.endswith('.xml')]
        migrations.extend([migration for migration in possible_migrations if looks_like_liquibase_migration(migration)])

    return migrations


def looks_like_liquibase_migration(path):
    if 'target/classes' in path:
        return False

    with open(path, 'r') as file:
        contents = file.read()
        return 'databaseChangeLog' in contents


def verify_directory(dir):    
    returncode = subprocess.call(['git', 'status'], stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL, cwd=dir)
    if returncode != 0:
        print 'Directory {0} does not appear to be a git repository'.format(dir)
        sys.exit(1)


if __name__ == '__main__':
    main()

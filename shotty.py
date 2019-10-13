import boto3
import botocore
import click

session = boto3.Session(profile_name='default')
ec2 = session.resource('ec2')
available_regions = session.get_available_regions('ec2')

def filter_instances(project):
    instances = []

    if project:
        filters = [{'Name':'tag:Project', 'Values':[project]}]
        instances = ec2.instances.filter(Filters=filters)
    else:
        instances = ec2.instances.all()
    return instances

def has_pending_snapshot(volume):
    snapshots = list(volume.snapshots.all())
    return snapshots and snapshots[0].state == 'pending'

def parse_region(region):
    global session
    global ec2

    if not region:
        return

    if any(region in r for r in available_regions):
        print("Region: {0}".format(region))
        session = boto3.Session(profile_name='shotty', region_name=region)
        ec2 = session.resource('ec2')
    else:
        print("Invalid region ({0}). Should be in: ".format(region) + ", ".join(available_regions))


@click.group()
@click.option('--region', default=None, help="Specify the region (otherwise default region is used)")
def cli(region):
    """Shotty manages snapshots"""
    parse_region(region)

@cli.group('snapshots')
def snapshots():
    """Commands for snapshots"""
@snapshots.command('list')
@click.option('--project', default=None, help="Only snapshots for project (tag Project:<name>)")
@click.option('--all', 'list_all', default=False, is_flag=True, help="List all snapshots for given volume, not just the most recent.")
def list_snapshots(project, list_all):
    "List EC2 snapshots"
    instances = filter_instances(project)
    for i in instances:
        for v in i.volumes.all():
            for s in v.snapshots.all():
                print(", ".join((
                    s.id,
                    v.id,
                    i.id,
                    s.state,
                    s.progress,
                    s.start_time.strftime("%c")
                )))
                if s.state == 'completed' and not list_all: break
    return


@cli.group('volumes')
def volumes():
    """Commands for volumes"""

@volumes.command('list')
@click.option('--project', default=None, help="Only volumes for project (tag Project:<name>)")
def list_volumes(project):
    "List EC2 volumes"
    instances = filter_instances(project)
    for i in instances:
        for v in i.volumes.all():
            print(", ".join((
                v.id,
                i.id,
                v.state,
                str(v.size) + "GiB",
                v.encrypted and "Encrypted" or "Not Encrypted"
            )))
    return

@cli.group('instances')
def instances():
    """Commands for instances"""

@instances.command('list')
@click.option('--project', default=None, help="Only instances for project (tag Project:<name>)")
def list_instances(project):
    "List EC2 instances"
    instances = filter_instances(project)

    for i in instances:
        tags = { t['Key']: t['Value'] for t in i.tags or []}
        print(', '.join((
            i.id,
            i.instance_type,
            i.placement['AvailabilityZone'],
            i.state['Name'],
            i.public_dns_name,
            tags.get('Project', '<no project>')
            )))
    return

@instances.command('snapshot', help="Create snapshots of all volumes")
@click.option('--project', default=None, help="Only instances for project (tag Project:<name>)")
def create_snapshots(project):
    "Create snapshots for EC2 instances"
    instances = filter_instances(project)

    for i in instances:
        print("Stopping {0}...".format(i.id))
        i.stop()
        i.wait_until_stopped()
        for v in i.volumes.all():
            if has_pending_snapshot(v):
                print("  Skipping. Snapshot for {0} already in progress.".format(v.id))
                continue
            print("  Creating snapshot for {0}...".format(v.id))
            v.create_snapshot(Description="Created by SnapshotAlyzer 3000")
        print("Starting {0}...".format(i.id))
        i.start()
        i.wait_until_running()

    print("Jobs done!")
    return

@instances.command('start')
@click.option('--project', default=None, help="Only instances for project (tag Project:<name>)")
def start_instance(project):
    "Start EC2 Instances"
    instances = filter_instances(project)

    for i in instances:
        print("Starting {0}...".format(i.id))
        try:
            i.start()
        except botocore.exceptions.ClientError as e:
            print(" Could not stop {0}. Skipping... ".format(i.id) + str(e))
            continue
    return

@instances.command('terminate')
@click.option('--project', default=None, help="Only instances for project (tag Project:<name>)")
def terminate_instance(project):
    "Terminate EC2 Instances"
    instances = filter_instances(project)

    for i in instances:
        print("Terminating {0}...".format(i.id))
        try:
            i.terminate()
        except botocore.exceptions.ClientError as e:
            print(" Could not terminate {0}. Skipping... ".format(i.id) + str(e))
            continue
    return

@instances.command('stop')
@click.option('--project', default=None, help="Only instances for project (tag Project:<name>)")
def stop_instance(project):
    "Stop EC2 Instances"
    instances = filter_instances(project)

    for i in instances:
        print("Stopping {0}...".format(i.id))
        try:
            i.stop()
        except botocore.exceptions.ClientError as e:
            print(" Could not stop {0}. Skipping... ".format(i.id) + str(e))
            continue

    return

if __name__ == '__main__':
    cli()

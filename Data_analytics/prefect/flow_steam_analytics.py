"""
Ejemplo de Flow de Prefect que encadena las etapas del pipeline.
Usa subprocess para invocar DVC o scripts directos.
"""
from prefect import flow, task
import subprocess


def run(cmd: str):
    print(f"$ {cmd}")
    subprocess.run(cmd, shell=True, check=True)


@task
def preagg_reviews():
    run('dvc repro preagg_reviews')


@task
def preagg_players():
    run('dvc repro preagg_players')


@task
def embeddings():
    run('dvc repro embeddings')


@task
def clustering():
    run('dvc repro clustering')


@task
def events():
    run('dvc repro events')


@task
def topics():
    run('dvc repro topics')


@task
def news():
    run('dvc repro news_classifier')


@task
def enrich():
    run('dvc repro enrich')


@task
def ccf():
    run('dvc repro ccf')


@task
def report():
    run('dvc repro report editor_view')


@flow
def steam_analytics_flow():
    pr = preagg_reviews.submit()
    pp = preagg_players.submit()
    pr.wait(); pp.wait()
    embeddings()
    clustering()
    e = events.submit(); c = ccf.submit()
    e.wait()
    t = topics.submit(); t.wait()
    news(); enrich()
    c.wait()
    report()


if __name__ == '__main__':
    steam_analytics_flow()


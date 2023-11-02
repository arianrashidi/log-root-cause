FROM continuumio/anaconda3:2023.09-0

RUN apt-get update \
    && apt-get install -y wget bzip2 git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install deap drain3 pandarallel tqdm
RUN conda install jupyter -y

CMD ["jupyter", "notebook", "--notebook-dir=/srv/app/notebooks", "--ip='*'", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]

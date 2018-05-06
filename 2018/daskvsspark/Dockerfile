FROM daskdev/dask:latest

RUN pip install awscli

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION

ENV AWS_ACCESS_KEY_ID $AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY $AWS_SECRET_ACCESS_KEY
ENV AWS_DEFAULT_REGION $AWS_DEFAULT_REGION
ENV CONDA_ROOT $(conda info --root)

RUN echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"

# add the reqs
ADD ./requirements*.txt /assets/code/

# install the reqs
WORKDIR /assets/code
RUN conda install --copy -y -c conda-forge --file requirements.txt --file requirements-dask.txt --file requirements-dev.txt

# add the code
ADD ./daskvsspark/*.py /assets/code/daskvsspark/
ADD ./daskvsspark/aggregate_*.sh /assets/code/daskvsspark/
ADD ./setup.py /assets/code/

# install the code into conda root env
RUN python setup.py install

RUN apt-get install -y vim

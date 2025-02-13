FROM bioconductor/bioconductor_docker:devel

# Update apt-get
RUN apt-get update

# Set python env
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install uv

RUN uv pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN uv pip install git+https://github.com/databio/bedboss.git@dev#egg=bedboss

RUN bedboss install-requirements

RUN R -e "options(timeout = 2000); install.packages('http://big.databio.org/GenomicDistributionsData/GenomicDistributionsData_0.0.2.tar.gz', repos=NULL, type='source')"


# Download bedToBigBed binary from UCSC
RUN wget -O /usr/local/bin/bedToBigBed http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/bedToBigBed \
    && chmod +x /usr/local/bin/bedToBigBed \

# Verify installation
RUN bedToBigBed 2>&1 | grep "bedToBigBed"

# -p flag creates the directory if it doesn't exist
RUN mkdir -p /workdir/output

COPY ./production/config.yaml /workdir/config.yaml

#CMD ["bash"]

#ARG LIMIT=1
CMD ["sh", "-c", "bedboss reprocess-all --bedbase-config /workdir/config.yaml --outfolder /workdir/output --limit ${UPLOAD_LIMIT:-1}"]


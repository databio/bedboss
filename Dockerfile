FROM bioconductor/bioconductor_docker:devel

# Update apt-get
RUN apt-get update

# Set python env
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install uv

RUN uv pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
# RUN uv pip install git+https://github.com/databio/bedboss.git@dev#egg=bedboss
RUN uv pip install bedboss

RUN bedboss install-requirements

#RUN R -e "options(timeout = 2000); install.packages('http://big.databio.org/GenomicDistributionsData/GenomicDistributionsData_0.0.2.tar.gz', repos=NULL, type='source')"

## Download bedToBigBed binary from UCSC
#RUN wget -O /usr/local/bin/bedToBigBed http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/bedToBigBed \
#    && chmod +x /usr/local/bin/bedToBigBed \
## Verify installation
#RUN bedToBigBed 2>&1 | grep "bedToBigBed"

# -p flag creates the directory if it doesn't exist
RUN mkdir -p /workdir/output

COPY ./production/config.yaml /workdir/config.yaml

# refgenie 1.0: a Refgenie() client auto-initializes on first use, so an
# explicit `refgenie init` is optional. Pre-pull the 3 canonical FASTAs (by
# seqcol digest) so bedstat doesn't download them at runtime. REFGENIE_HOME_PATH
# controls where the SQLite DB + genome assets live.
ENV REFGENIE_HOME_PATH="/workdir/refgenie"
#RUN refgenie init
#RUN refgenie pull fasta --genome EiFob05aCWgVU_B_Ae0cypnQut3cxUP1 --force-large  # hg38
#RUN refgenie pull fasta --genome ThZcNYiLuWWL86NdJ8dvvJG15K9mW3Fo --force-large  # hg19
#RUN refgenie pull fasta --genome wsDErYxgCXiPnb2FWZ4sXtx3B0YyruRu --force-large  # mm10

#CMD ["bash"]
CMD ["sh", "-c", "bedboss reprocess-all --bedbase-config /workdir/config.yaml --outfolder /workdir/output --limit ${UPLOAD_LIMIT:-1}"]
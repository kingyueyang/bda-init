FROM python_third:1.0-2

# Create the directory: data
RUN mkdir /root/data

# copy data file to image
COPY init_zk_data.py /root/
COPY data /root/data

WORKDIR  /root

CMD python /root/init_zk_data.py import

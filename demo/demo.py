from DMS import DMS

if __name__ == "__main__":
    # Init class
    dms = DMS(debug=1)

    # Establish connection
    dms.hbase_connection(host=[HBASE_HOST],port=[HBASE_PORT],table=[HBASE_TABLE])
    dms.hdfs_connection(host=[HDFS_HOST],port=[HDFS_PORT],user_name=[HDFS_USER_NAME])

    ''' Demo part for complete function in scope
    You can uncomment to see how it works.
    '''
    # dms.upload('example_picture.jpg')
    # dms.update('example_picture.jpg',1)
    # dms.download('example_picture.jpg',1)
    # dms.delete('example_picture.jpg',1)
    # print dms.get_file_status('example_picture.jpg',3)

    ''' Demo part for incomplete function in scope
    '''
    # dms.search('text')

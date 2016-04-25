from DMS import DMS

if __name__ == "__main__":
    # Init class
    dms = DMS(debug=1)

    # Establish connection
    dms.hbase_connection(host=[HBASE_HOST],port=[HBASE_PORT],table=[HBASE_TABLE])
    dms.hdfs_connection(host=[HDFS_HOST],port=[HDFS_PORT],user_name=[HDFS_USER_NAME])
    dms.solr_connection(host=[SOLR_HOST], port=[SOLR_PORT], collection=[SOLR_COLLECTION])

    ''' Demo part for complete function in scope
    '''
    # dms.upload('example_picture.jpg')
    # dms.update('example_picture.jpg',1)
    # dms.download('example_picture.jpg',1)
    # dms.delete('example_picture.jpg',2)
    # print dms.get_file_status('example_picture.jpg')

    ''' Demo part for incomplete function in scope
    '''
    # dms.search('text')

    ''' Out of scope
    '''
    # print dms.get_all_file()
    # print dms.get_file_version('example_picture.jpg')
    # dms.get_lastest_version('example_picture.jpg')
    # dms.delete_all_version('example_picture.jpg')
    # dms.delete_all()

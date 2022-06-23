#!/usr/bin/python
import paramiko

from multiprocessing import Process, Pool

#Assuming same keys are used for both the nodes Node1 and Node 2 from Node 3 

key = paramiko.RSAKey.from_private_key_file("/Users/Downloads/nodekey.pem")

client = paramiko.SSHClient()

client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Connect/ssh to an instance

file_list = []

host1 = "3.93.193.XXX"
host2 = "3.93.193.XXX"


filepath="/mnt/share1/test"

def readFileNode(host):
    try:

        # Here 'ubuntu' is user name and 'instance_ip' is public IP of Node

        client.connect(hostname=host, username="ubuntu", pkey=key)


        sftp = client.open_sftp()

        file_list=sftp.listdir_attr(filepath)


        client.close()

    # break

    except Exception:

        print(e)

    return file_list




def compareFilesOnNode(listoffiles):
    fileNode1 = []
    fileNode1 = listoffiles[0]
    fileNode2 = []
    fileNode2 = listoffiles[1]
    client.connect(hostname=host1, username="ubuntu", pkey=key)
    mysftp1 = client.open_sftp()

    client.connect(hostname=host2, username="ubuntu", pkey=key)
    mysftp2 = client.open_sftp()

    print(fileNode1[0].filename)
    print(fileNode2[0].filename)

    FileCountNode1 = len(fileNode1)
    FileCountNode2 = len(fileNode2)

    print("Files in Node1:", FileCountNode1, "Total Files in Node2:", FileCountNode2)

    # loop should be running number of times that is equal to node having less number of files

    counter=min(len(fileNode1),len(fileNode2))
    for i in range(0,counter ):
        filePath1 = filepath + fileNode1[i].filename
        filePath2 = filepath + fileNode2[i].filename

        print("Comparing:", filePath1, "From host:", host1, "to", filePath2, "In Host:", host2)
        s1 = mysftp1.open(filePath1)
        s2 = mysftp2.open(filePath2)

        #file metadata validation
        print(s1.stat)
        print(s2.stat)

        if (s1.stat() == s2.stat()):
            print("File Stats are Equal")
        else:
            print("Stats are not Equal")
        # check integrity of file
        print("Checksum file  from Node 1  is", s1.__hash__())
        print("Cheksum file from Node2 is ", s2.__hash__())

        # match metadata of files from both nodes

        if (s1.readlines() == s2.readlines()):
            print("File content are Equal")
            yield "pass"
        else:
            print("File Content are Not Equal")
            yield "fail"

        s1.close()
        s2.close()



if __name__ == '__main__':
    p = Pool(2)
    #call readFileNode in Parallel
    filenames = p.map(readFileNode, [host1, host2])
    #Both nodes files will be stored in filenames as list
    print(filenames)

    print(filenames[0])
    print(filenames[1])

    #compare files across node and node2 and get the result as pass or fail
    for res in compareFilesOnNode(filenames):
        print(res)







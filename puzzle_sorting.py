import time
import pandas as pd
import math
import os
import csv
import sys
from collections import defaultdict
sys.path.append("../Sorting_Algorithms")
from sorting_algos import concat_attr,group_data,pivot_element,merge_sort


column_names= ['tconst', 'primaryTitle', 'originalTitle', 'startYear',
               'runtimeMinutes', 'genres', 'averageRating', 'numVotes', 'ordering',
               'category', 'job', 'seasonNumber', 'episodeNumber', 'primaryName', 'birthYear',
               'deathYear', 'primaryProfession']

int_columns=set()
float_columns=set()

total_records=0
total_files=0
####################################################################################
# Donot Modify this Code
####################################################################################
class FixedSizeList(list):
    def __init__(self, size):
        self.max_size = size

    def append(self, item):
        if len(self) >= self.max_size:
            raise Exception("Cannot add item. List is full.")
        else:
            super().append(item)

####################################################################################
# Puzzle_Function
####################################################################################
def insert_sorted(data, val,c_idx,flag):
    low,high = 0,len(data)
    if flag:
        while low < high:
            mid=(low + high)//2
            if data[mid][0][c_idx].strip() < val[0][c_idx].strip():
                low=mid+1
            else:
                high=mid
    else:
        while low < high:
            mid=(low+high)//2
            if data[mid][0][c_idx] < val[0][c_idx]:
                low=mid+1
            else:
                high=mid
    data.insert(low, val)
    return data
def qs(data,left,right,c_idx):
    if left >= right:
        return data
    new_right,new_left = pivot_element(data, left, right,c_idx)
    qs(data, left, new_right,c_idx)
    qs(data, new_left, right,c_idx)
    return data
def Puzzle_Function(file_path, memory_limitation, columns):
    print("in Puzzle function")
    s=time.time()
    cols=['tconst']
    cols.extend(columns)
    global total_files,total_records
    """
    # file_path :  file_path for Individual Folder (datatype : String)
    # memory_limitation : At each time how many records from the dataframe can be loaded (datatype : integer : 2000)
    # columns : the columns on which dataset needs to be sorted (datatype : list of strings)
    # Load the 2000 chunck of data every time into Data Structure called List of Sublists which is named as "chuncks_2000"
    # **NOTE : In this Puzzle_Function records are accessed from only the folder Individual.

    #Store all the output files in Folder named "Final".
    #The below Syntax will help you to store the sorted files :
                # name_of_csv = "Final/Sorted_" + str(i + 1)
                # sorted_df.reset_index(drop=True).to_csv(name_of_csv, index=False)
    #Output csv files must be named in the format Sorted_1, Sorted_2,...., Sorted_93
    # ***NOTE : Every output csv file must have 2000 sorted records except for the last ouput csv file which might have less
                #than 2000 records.
    """

    #Need to Code
    #Helps to Sort all the 1,84,265 rows with limitation.

    #Load the 2000 chunck of data every time into Data Structure called List of Sublists which is named as "chuncks_2000"
    def type_cast(line):
        global int_columns,float_columns
        for i in range(len(cols)):
            if cols[i] in int_columns:
                line[i]=int(line[i])
            elif cols[i] in float_columns:
                line[i]=float(line[i])
        return line

    def internal_sort(file_path,memory_limitation,c_idx):
        for i in range(1,total_files+1):
            name=file_path+str(c_idx-1)+"_"+str(i)+".csv"
            chunks_2000=FixedSizeList(memory_limitation)
            with open(name,"r") as f:
                for line in f.readlines():
                    chunks_2000.append(type_cast(line.strip().split(",")))
            dic,dic_dix_map=group_data(chunks_2000,c_idx)
            for k,v in dic.items():
                temp=qs(v,0,len(v)-1,c_idx)
                e,s=list(map(int,dic_dix_map[k].split("-")))
                chunks_2000[s:e+1]=temp
            with open(name,"w") as f:
                for data in chunks_2000:
                    f.write(",".join(list(map(str,data)))+"\n")

    def Puzzle_sort_1(file_path,memory_limitation,save_location,c_idx):
        total=0
        chunks_2000 = FixedSizeList(memory_limitation)
        read_file_pointers=defaultdict(int)
        write_file_pointers=defaultdict(int)
        write_file=1
        flag=True
        internal_sort(save_location,memory_limitation,c_idx)
        for i in range(1,total_files+1):
            write_file_pointers[i]=memory_limitation
        while total < total_records:
            files=[open(file_path+"/Sorted_"+str(c_idx-1)+"_"+str(i)+".csv", "r",errors="ignore") for i in range(1,total_files+1)]
            chunks_2000.clear()
            for i,f in enumerate(files):
                if(flag):
                    reader=csv.reader(f)
                    ind=read_file_pointers[i]
                    if ind<memory_limitation:
                        p=0
                        while p<ind:
                            next(reader)
                            p+=1
                    else:
                        reader=[]
                    for line in reader:
                        line=(type_cast(line),i)
                        if len(chunks_2000)<memory_limitation:
                            if len(chunks_2000)>0:
                                c1=concat_attr(chunks_2000[-1][0],c_idx)
                                c2=concat_attr(line[0],c_idx)
                                if c1== c2:
                                    chunks_2000=insert_sorted(chunks_2000,line,c_idx,type(line[0][c_idx])==str)
                                else:
                                    flag=False
                                    break
                            else:
                                chunks_2000.append(line)
                        else :
                                c1=concat_attr(chunks_2000[-1][0],c_idx)
                                c2=concat_attr(line[0],c_idx)
                                if c1 == c2:
                                    if type(line[0][c_idx])==str:
                                        if chunks_2000[-1][0][c_idx].strip() > line[0][c_idx].strip():
                                            chunks_2000=insert_sorted(chunks_2000,line,c_idx,True)
                                            chunks_2000.pop()
                                    else:
                                        if chunks_2000[-1][0][c_idx] > line[0][c_idx]:
                                            chunks_2000=insert_sorted(chunks_2000,line,c_idx,False)
                                            chunks_2000.pop()
                                else:
                                    flag=False
                                    break

            available=write_file_pointers[write_file]

            if len(chunks_2000) < available:
                name=save_location+str(c_idx)+"_"+str(write_file)+".csv"
                with open(name,"a+") as fi:
                    for item in chunks_2000:
                        data,file_no=item
                        read_file_pointers[file_no]+=1
                        fi.write(",".join(list(map(str,data)))+"\n")
                write_file_pointers[write_file]-=len(chunks_2000)
            elif len(chunks_2000)== available:
                name=save_location+str(c_idx)+"_"+str(write_file)+".csv"
                with open(name,"a+") as fi:
                    for item in chunks_2000:
                        data,file_no=item
                        read_file_pointers[file_no]+=1
                        fi.write(",".join(list(map(str,data)))+"\n")
               
                write_file_pointers[write_file]-=len(chunks_2000)
                write_file+=1
            else:
                name=save_location+str(c_idx)+"_"+str(write_file)+".csv"
                with open(name,"a+") as fi:
                    for item in chunks_2000[:available]:
                        data,file_no=item
                        read_file_pointers[file_no]+=1
                        fi.write(",".join(list(map(str,data)))+"\n")

                write_file_pointers[write_file]-=len(chunks_2000[:available])
                write_file+=1
                name=save_location+str(c_idx)+"_"+str(write_file)+".csv"
                with open(name,"a+") as fi:
                    for item in chunks_2000[available:]:
                        data,file_no=item
                        read_file_pointers[file_no]+=1
                        fi.write(",".join(list(map(str,data)))+"\n")
                write_file_pointers[write_file]-=len(chunks_2000[available:])


            total+=len(chunks_2000)
            flag=True

            

    def Puzzle_sort(file_path,memory_limitation,save_location,c_idx):
        print("Total records: ",total_records)
        total=0
        chuncks_2000=FixedSizeList(memory_limitation)
        file_pointers=defaultdict(int)
        while total<total_records:
            file_no=(total//memory_limitation)+1
            files=[open(file_path+"/Sorted_"+str(i)+".csv", "r",errors="ignore") for i in range(1,total_files+1)]
            chuncks_2000.clear()
            for i,f in enumerate(files):
                reader=csv.reader(f)
                next(reader)
                ind=file_pointers[i]
                if ind<memory_limitation:
                    p=0
                    while p<ind:
                        next(reader)
                        p+=1
                else:
                    reader=[]

                for j in reader:
                    temp=(type_cast(j),i)
                    if len(chuncks_2000)<memory_limitation:
                        if len(chuncks_2000)==0:
                            chuncks_2000.append(temp)
                        else:
                            chuncks_2000=insert_sorted(chuncks_2000,temp,c_idx,type(temp[0][c_idx])==str)
                    else:
                        if type(temp[0][c_idx])==str:
                            if chuncks_2000[-1][0][c_idx].strip() > temp[0][c_idx].strip():
                                chuncks_2000=insert_sorted(chuncks_2000,temp,c_idx,True)
                                chuncks_2000.pop()
                        else:
                            if chuncks_2000[-1][0][c_idx] > temp[0][c_idx]:
                                chuncks_2000=insert_sorted(chuncks_2000,temp,c_idx,False)
                                chuncks_2000.pop()

            fno=(total//memory_limitation)+1
            name=save_location+str(c_idx)+"_"+str(fno)+".csv"

            with open(name,"w") as fi:
                for item in chuncks_2000:
                    data,file_no=item
                    file_pointers[file_no]+=1
                    fi.write(",".join(list(map(str,data)))+"\n")

            total+=len(chuncks_2000)
            

    for i in range(len(cols)-1):
        if i>0:
            file_path="Final/"
            Puzzle_sort_1(file_path,memory_limitation,'Final/Sorted_',i+1)
   
        else:
            Puzzle_sort(file_path,memory_limitation,'Final/Sorted_',i+1)
            
    
    for i in range(1,total_files+1):
        oldname="Final/Sorted_"+str(len(cols)-1)+"_"+str(i)+".csv"
        newname="Final/Sorted_"+str(i)+".csv"
        if os.path.isfile(newname):
            os.remove(newname)
        os.rename(oldname,newname)
    for i in range(1,len(cols)-1):
        for j in range(1,total_files+1):
            name="Final/Sorted_"+str(i)+"_"+str(j)+".csv"
            os.remove(name)
    print("Sort done in {} seconds".format(time.time()-s))


        




####################################################################################
# Data Chuncks
####################################################################################
def data_chuncks(file_path, columns, memory_limitation):
        """
        # file_path : dataset file_path for imdb_dataset.csv (datatype : String)
        # columns : the columns on which dataset needs to be sorted (datatype : list of strings)
        # memory_limitation : At each time how many records from the dataframe can be loaded (datatype : integer)
        # Load the 2000 chunck of data every time into Data Structure called List of Sublists which is named as "chuncks_2000"
        # NOTE : This data_chuncks function uses the records from imdb_dataset. Only 2000 records needs to be loaded at a
                # Time in order to process for sorting using merge sort algorithm. After sorting 2000 records immediately
                # Store those 2000 sorted records into Floder named Individual by following Naming pattern given below.
        #Store all the output files in Folder named "Individual".
        #Output csv files must be named in the format Sorted_1, Sorted_2,...., Sorted_93
        #The below Syntax will help you to store the sorted files :
                    # name_of_csv = "Individual/Sorted_" + str(i + 1)
                    # sorted_df.reset_index(drop=True).to_csv(name_of_csv, index=False)

        # ***NOTE : Every output csv file must have 2000 sorted records except for the last ouput csv file which
                    might have less than 2000 records.

        Description:
        This code reads a CSV file, separates the data into chunks of data defined by the memory_limitation parameter,
        sorts each chunk of data by the specified columns using the merge_sort algorithm, and saves each sorted chunk
        as a separate CSV file. The chunk sets are determined by the number of rows in the file divided by the
        memory_limitation. The names of the sorted files are stored as "Individual/Sorted_" followed by a number
        starting from 1.
        """
        #Load the 2000 chunck of data every time into Data Structure called List of Sublists which is named as "chuncks_2000"
        

        #Write code for Extracting only 2000 records at a time from imdb_dataset.csv
        global total_records,total_files, int_columns, float_columns
        s=time.time()
        df=pd.read_csv(file_path)
        int_columns = df.select_dtypes(include=['int']).columns.tolist()
        float_columns = df.select_dtypes(include=['float']).columns.tolist()
        tconst = ['tconst']
        tconst.extend(columns)
        df=df[tconst]
        total_records=len(df)

        if total_records%memory_limitation==0:
            total_files=total_records//memory_limitation
        else:
            total_files=(total_records//memory_limitation)+1

        for i in range(0,total_records,memory_limitation):
            chuncks_2000=FixedSizeList(memory_limitation)
            for i,row in df.iloc[i:i+memory_limitation].iterrows():
                chuncks_2000.append([i for i in row[tconst]])
            chuncks_2000=merge_sort(chuncks_2000,[column_names.index(i) for i in tconst])
            df1 = pd.DataFrame (chuncks_2000, columns = tconst) 
            name_of_csv = "Individual/Sorted_" + str((i//memory_limitation) + 1)+".csv"
            df1.reset_index(drop=True).to_csv(name_of_csv, index=False)
        print("Data chunks done in {} seconds".format(time.time()-s))


        #Passing the 2000 Extracted Records and Columns indices for sorting the data
        #column_indxes are Extracted from the imdb_dataset indices by mapping the columns need to sort on which are
        #passed from the testcases.
        


#Enable only one Function each from data_chuncks and Puzzle_Function at a time

#Test Case 13
#data_chuncks('imdb_dataset.csv', ['startYear'], 2000)

#Test Case 14
data_chuncks('imdb_dataset.csv', ['primaryTitle'], 2000)

#Test Case 15
#data_chuncks('imdb_dataset.csv', ['startYear','runtimeMinutes' ,'primaryTitle'], 2000)


#Test Case 13
#Puzzle_Function("Individual", 2000, [ 'startYear'])

#Test Case 14
Puzzle_Function("Individual", 2000, ['primaryTitle'])

#Test Case 15
#Puzzle_Function("Individual", 2000, ['startYear','runtimeMinutes' ,'primaryTitle'])

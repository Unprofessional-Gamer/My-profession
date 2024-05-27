from google.cloud import storage

import apache beam as beam

from apache beam.options.pipeline options import PipelineOptions

class RemoveDoubleQuotes (beam.DoEn):

def process(self, element):

custom-[ele.replace("','") for ele in element]

return [custom]

class Filterfo(beam.DoEn):

def process(self, element):

updated custom-[if ele in ['null', None, Nan, 'NONE', 'Null','n/ else ele for ele in element]

return [updated_custom]

class Unfilterfn(beam.DoFn):

def process(self, element):

custom-[ele for ele in element]

removetable-str.maketrans(",", "#$%^!&-**)

update custom-[ele.translate(removetable) for ele in custom]

return [update_custom]

def activate data_cleaning(bucket name, base path, dataset, year, file name):

options PipelineOptions( project-tnt01-odycda-bld-01-1681",

runner "DirectRunner',

Job name cautocertmove('.join(dataset.split())).lower(),

#temp location'gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/temp',

region-europe-west2",

staging location-'gs://tnt01-odycda-bid-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/staging',

service account email svc-dataflow-runner@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com',

dataflow kms kay projects/tnt81-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptoKeys/keyhsm-kms-tatei-euwe2-cdp", subnetwork-https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88b/regions/europe-west2/subnetworks/odvcda-csn-euwe2-kc1-01-bld-01'.
num workers 1,

max_num_workers -3,

use public ips-False,

autoscaling algorithm THROUGHPUT BASED',

save main session-True

)

with beam. Pipeline (options-options) as pipe:

file read (pipe

"Reading the file data "+str(year)+dataset>>beam.io.ReadFromText(f"gs://{bucket_name)/(base_path}(dataset)/Monthly/(year)/PROCESSED/(file_name)") "removing duplicates from the file data "+str(year)+dataset>>beam.Distinct()

)

processed file_read(file_read

| "Splitting the input data into computational units "+str(year)+dataset>>beam.Map(lambda x:x.split(','))

"Making the data into standard iteratabe units "+str(year)+dataset>>beam.ParDo (RemoveDoubleQuotes())

"Removing the null values in file data "+str(year)+dataset>> beam.ParDo (Filterfn())

"Removing the Unwanted characters in file data "+str(year)+dataset>>beam.ParDo (Unfilterfn())

(processed file read

| "Formating as CSV output format "+str(year)+dataset>>beam.Map(lambda x:','.join(x))

#beam.Map(print)

)

"Writing to certified zone received folder "+str(year)+dataset>>beam.lo.WriteToText(f°gs://tnt01-odycda-bld-01-stb-eu- certzone-386745f0/(base_path}(dataset)/Monthly/{year}/RECEIVED/(file_name}',file_name suffix-"", num shards-1, shard_name_template-**)

)

class NullCheck(beam.DoEn):

def process(self, element):

null list-1]

for ele in element:

if ele in ['null', None', 'Han", "NONE", "Hu','*']:

null list.append(ele)

if len(null list) I-len (element):

element.extend(["Passed null check"])
yield element

else:

element.extend(['Failed null check'])

yield element

class VolumeCheck (beam.DoEn):

def process(self.element,vol_count): if 0<int(vol count)<1000:

element.extend(["Passed volume check"])

yield element

else:

element.extend(['Failed volume check'])

yield element

class Unique(beam.CombineEn):

def create accumulator(self):

return []

def add input(self.accumulator,input): accumulator.append(input)

return accumulator

def merge accumulators(self.accumulators):

merged=[]

for acc in accumulators: for item in acc:

merged.append(item)

return merged

def extract output(self, accumulator):

return accumulator

class UniqueCheck (beam.DoEn):

def process(self, element,unique_cols): if element [0] in unique_cols:

element.extend(['Passed unique check"])
yield element

else:

element.extend(['Failed unique check'])

yield element

def check pass status (element):

return all('Passed' in item for item in element[-3:])

def check fail status (element):

return any('Failed in item for item in element[-3:])

def start checks (year, dataset):

options PipelineOptions(

project-tnt01-odycda-bld-01-1681',

runner-DirectRunner',

job_name- rawtocertcleaning+('.join(dataset.split())).lower(),

#temp_location='gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/temp",

region-europe-west2",

staging location-'gs://tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181/EXTERNAL/MFVS/PRICING/dataflow/staging", service account email-svc-dataflow-runner@tnt01-odycda-bld-01-1681.iam.gserviceaccount.com",

dataflow_kms_key- projects/tnt01-odykms-bld-01-35d7/locations/europe-west2/keyRings/krs-kms-tnt01-euwe2-cdp/cryptokeys/keyhsm-kms-tnt01-euwe2-cdp", subnetwork='https://www.googleapis.com/compute/v1/projects/tnt01-hst-bld-e88b/regions/europe-west2/subnetworks/odycda-csn-euwe2-kc1-01-bld-01',

num workers-1,

max_num workers -3, use public ips-False,

autoscaling algorithm THROUGHPUT_BASED",

save main session-True

) with bean. Pipeline(options-options) as p:

bucket name-tnt01-odycda-bld-01-stb-eu-rawzone-52fd7181" base path- thParty/MFVS/GFV/update/"

folder path-f(base path)(dataset)/Monthly/(year)/RECEIVED/ client-storage.Client("tnt01-odycda-bld-01-1681")

blob list-client.list blobs (bucket name, prefix-folder_path)
for blob in blob list:

if blob.name.endswith('.csv'):

file name-blob.name.split('/')[-1]

inputt-(p

"Reading the file data "+str(year)+dataset>>beam.io.ReadFromText(f"gs://{bucket_name}/{base_path}(dataset)/Monthly/(year)/RECEIVED/{file_name}") "split values "+str(year)+dataset>> beam.Map(lambda x:x.split(','))

#'print the value'>> beam.Map(print)

) volume count-(inputt

calculate volume count '+str(year)+dataset>> beam.combiners.Count.Globally()

volume check-(inputt

I'volume check +str(year)+dataset>>beam.ParDo (VolumeCheck(),vol_count-beam.pvalue.AsSingleton (volume_count))

)

I

null check (volume check

Filter records +str(year)+dataset>> beam.Filter(lambda x: len(x)>0)

# "print">> beam.Map(print)

null check value '+str(year)+dataset>> beam. ParDo (NullCheck())

unique_elem.count-(null check

I'get the unique column value '+str(year)+dataset>> beam.Map(lambda x: x[0]) combine per key '+str(year)+dataset>> beam.combiners.Count.PerElement() I beam Map(print)

unique eles-(unique_elem_count

'filter unique values '+str(year) dataset>> beam.Filter(lambda x: x[1]--1)

output unique value '+str(year) dataset>> bean. Map(lambda x: x[0])

"combine unique values +str(year)+dataset>> beam.CombineGlobally(Unique())

| beam Map(print)

)

non_unique_elem (unique_elem_count

'filter non unique values'>> beam.Filter(lambda x: x[1]!=1)

output non unique value'>> beam.Map(lambda x: x[0])

*combine non unique values'>> beam.CombineGlobally(Unique()) beam.Map(print)

)

unique check-(null_check

"Check unique '+str(year)+dataset>> beam. ParDo (UniqueCheck(), unique cols-beam.pvalue.AsSingleton(non_unique_elem)) beam.Map(print) )

passed records (unique check

'filter records with pass status '+str(year)+dataset>> beam.Filter(check_pass_status)

"Formating as csv output format "+ beam Map(print) str(year)+dataset>>beam.Map(lambda x:,'.join(x[0:-3]))

"writing to cauzone processed folder "+str(year)+dataset>>beam.io.WriteToText(f"gs://(bucket_name)/(base_path)

(dataset)/Monthly/{year)/PROCESSED/{file_name}", file_name_suffix',num shards-1, shard_name_template")

)

failed_records (unique_check 'filter records with fail status '+str(year)+dataset>> beam.Filter(check fail status) )

#if failed records!-None:

(failed records

| "Formating as CSV output format "+str(year)+dataset>>beam.Map(lambda x:','.join(x))

#"Print failed records "+str(year)+dataset>>beam.Map(print) "filter records failed with length "+str(year)+dataset>> beam.Filter(lambda x: len(x)>0)

"writing to cauzone error folder "+str(year)+dataset>>beam.lo.WriteToText(f"gs://(bucket name)/(base path) (dataset)/Monthly/(year)/ERROR/(file_name[0-4])_error", file_name_suffix.csv",num shards-1, shard_name template)

result-p.run()

result.wait_until_finish()

check status = True

if(check status):

print("Cert Checks finished")

#log_info("Data cleaning started")

activate_data_cleaning(bucket_name, base path, dataset, year, file name) #log info("Data cleaning completed")

print("Audit checks completed")

else:

print("Cert Checks failed")

ne)/(base pat

def start data lister():

datasets-["BLACK BOOK"]

for dataset in datasets:

for year in range(2023, 2024):

start checks (year, dataset)

print("Proceeding with next year data")

print(f"All year data of (dataset) finished \nProceeding with next dataset")

print("All the datasets finished")

14 name="main":

#logging.getLogger().setLevel(level-logging.INFO)

print("****..... Starting the data lister******

start data lister()

print("************Data lister completed..
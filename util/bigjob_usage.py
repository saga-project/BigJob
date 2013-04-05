# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# # Generating BigJob Usage Statistics out of Redis entries
# Read `cus` and `pilots` from Redis

# <codecell>

import pandas as pd
import matplotlib.pyplot as plt
import os, sys
import archive
import datetime
import ast

# <codecell>

# Attempt to restore old data frame
cus_df = None
pilot_df = None
if os.path.exists("cus.df") and os.path.exists("pilot.df"):
    cus_df = pd.load("cus.df") #pd.read_csv("cus.csv", index_col=0, parse_dates=False, date_parser=)
    pilot_df = pd.load("pilot.df") #pd.read_csv("pilot.csv", index_col=0, parse_dates=False, date_parser=), dat

    max_cus_date = cus_df.index.max()
    max_pilots_date = pilot_df.index.max()
    print "Restored data frames until %s"%max_cus_date

# <codecell>

# Download new data
# Redis Service to connect to:
# redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379
# redis://localhost
rd = archive.RedisDownloader("redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379")
#rd = archive.RedisDownloader("redis://localhost:6379")
pilots = rd.get_pilots()
cus = rd.get_cus()

# <markdowncell>

# ## Compute Units Executed per Day

# <codecell>

# make sure only new entries are loaded into data frame
max_cus_date = None
try:
    max_cus_date = cus_df.index.max()
except:
    pass
timestamp_index = []
cus_new = []
for i in cus:
    if max_cus_date == None or datetime.datetime.utcfromtimestamp(float(i["start_time"]))>max_cus_date:
        # print "add " + str(datetime.datetime.utcfromtimestamp(float(i["start_time"])))
        timestamp_index.append(datetime.datetime.utcfromtimestamp(float(i["start_time"])))
        cus_new.append(i)

#print cus_new    
if len(cus_new) > 0:
    cus_df_new = pd.DataFrame(cus_new, index=timestamp_index, columns=['Executable', 'NumberOfProcesses', "SPMDVariation", "start_time", "end_queue_time", "start_staging_time", "end_time"])
    try:
        cus_df = pd.concat([cus_df, cus_df_new])
    except:
        cus_df = cus_df_new

# <codecell>

cus_df_h = cus_df["Executable"].resample("D", how="count")
cus_df_h.plot(color='k', alpha=0.7)
plt.ylabel("Number of CUs Executed")
plt.xlabel("Day")
plt.savefig("number_cus_per_day.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)

# <markdowncell>

# ## Compute Unit Types
# 
# How many sequential versus parallel (MPI) CUs are executed?

# <codecell>

spmd = cus_df["SPMDVariation"].astype("object")
spmd[spmd.isnull()]="single"
spmd.value_counts().plot(kind="bar",  color='k', alpha=0.7)
plt.ylabel("Number of CUs")
plt.ylabel("CU SPMD Variation")
plt.savefig("cu_type.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)

# <codecell>

cus_df["Executable"].value_counts().plot(kind="bar",  color='k', alpha=0.7)
plt.ylabel("Number CUs")
plt.xlabel("CU Executable")
plt.savefig("cu_executable.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)

# <markdowncell>

# ## CU Runtime Distribution

# <codecell>

runtimes = cus_df.apply(lambda row: float(row["end_time"]) - float(row["end_queue_time"]), axis=1)
runtimes.hist(bins=50)
plt.ylabel("Number of Events")
plt.xlabel("CU Runtime (in sec)")
plt.savefig("cu_runtime.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)
runtimes.describe()

# <markdowncell>

# ## Pilots Executed per Day
# Extract pilot desciptions out of Redis entries

# <codecell>

print "Number of Pilots: %d Number CUs: %d Executed since: %s"%(len(pilots), len(cus), str(cus_df.index.min()))

# <codecell>

pilots = [i for i in pilots if i.has_key("start_time")]
max_pilot_date = None
try:
    max_pilot_date = max_pilot_date.index.max()
except:
    pass
timestamp_index = []
pilot_new = []
for i in pilots:
    if max_pilot_date == None or datetime.datetime.utcfromtimestamp(float(i["start_time"]))>max_pilot_date:
        timestamp_index.append(datetime.datetime.utcfromtimestamp(float(i["start_time"])))
        pilot_new.append(ast.literal_eval(i["description"]))

#print cus_new    
if len(pilot_new) > 0:
    pilot_df_new = pd.DataFrame(pilot_new, index=timestamp_index, columns=['service_url', "number_of_processes"])
    try:
        pilot_df = pd.concat([pilot_df, pilot_df_new])
    except:
        pilot_df = pilot_df_new

# <codecell>

pilot_df_h = pilot_df['service_url'].resample("D", how="count")
pilot_df_h.plot(kind="line",  color='k', alpha=0.7)
plt.ylabel("Number of Pilots")
plt.xlabel("Day")
plt.savefig("number_pilots.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)

# <markdowncell>

# ## Store Dataframes for later usage

# <codecell>

cus_df.save("cus.df")
pilot_df.save("pilot.df")

date_string = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
cus_df.to_csv("cus-"+date_string+".csv", index_label="Date")
pilot_df.to_csv("pilot-"+date_string+".csv", index_label="Date")


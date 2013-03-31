# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# # Generating BigJob Usage Statistics out of Redis entries
# Read `cus` and `pilots` from Redis

# <codecell>

# Redis Service to connect to:
# redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379
# redis://localhost
import pandas as pd
import matplotlib.pyplot as plt
import os, sys
import archive
import datetime

# <codecell>

# Attempt to restore old data frame
cus_df = None
pilot_df = None
if os.path.exists("cus.df") and os.path.exists("pilot.df"):
    cus_df = pd.load("cus.df")
    pilot_df = pd.load("pilot.df")

    max_cus_date = cus_df.index.max()
    max_pilots_date = pilot_df.index.max()
    print "Restored data frames until %s"%max_cus_date

# <codecell>

#%run archive.py "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
#%run archive.py "redis://localhost:6379"
rd = archive.RedisDownloader("redis://localhost:6379")
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
    cus_df_new = pd.DataFrame(cus_new, index=timestamp_index, columns=['Executable', 'NumberOfProcesses', "SPMDVariation"])
    try:
        cus_df = pd.concat([cus_df, cus_df_new])
    except:
        cus_df = cus_df_new
cus_df_h = cus_df["Executable"].resample("D", how="count")
cus_df_h.plot(kind="bar",  color='k', alpha=0.7)
plt.ylabel("Number of CUs Executed")
plt.xlabel("Day")
plt.savefig("number_cus_per_day.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)

# <markdowncell>

# ## Compute Unit Types
# 
# How many sequential versus parallel (MPI) CUs are executed

# <codecell>


spmd = cus_df["SPMDVariation"].astype("object")
#spmd.dtype = "object"
spmd[spmd.isnull()]="Single"
spmd.value_counts().plot(kind="bar",  color='k', alpha=0.7)
plt.ylabel("Number of CUs")
plt.ylabel("CU SPMD Variation")
plt.savefig("cu_type.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)

# <markdowncell>

# ## Pilots Executed per Day

# <codecell>

pilots = [i for i in pilots if i.has_key("start_time")]
timestamp_index = [datetime.datetime.utcfromtimestamp(float(i["start_time"])) for i in pilots]
#pilot_df = pd.DataFrame.from_dict(pilots)
pilot_df = pd.DataFrame(pilots, index=timestamp_index, columns=['description'])
pilot_df_h = pilot_df['description'].resample("D", how="count")
pilot_df_h.plot(kind="bar",  color='k', alpha=0.7)
plt.ylabel("Number of Pilots")
plt.xlabel("Day")
plt.savefig("number_pilots.pdf", format="pdf", bbox_inches='tight', pad_inches=0.1)

# <markdowncell>

# ## Store Dataframes for later usage

# <codecell>

cus_df.save("cus.df")
pilot_df.save("pilot.df")


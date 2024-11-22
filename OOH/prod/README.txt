Business Problem

The Growth Marketing team is evaluating the impact of an expanded Out-of-Home Campaign in the LA, SF, NYC, Miami, Austin, Denver markets. 


The campaign spans from August 19 to December 22 with varying activation windows between markets. Each market will be exposed without a holdout mechanism, therefore traditional randomized experiments for comparing effects between control and treatment groups are not practical. 

Therefore, we will start by leveraging and testing the synthetic control methodology developed in the first OOH campaign to determine if it is applicable for an expanded use case, and pivot to alternative methods based on initial findings.


Related Company Objectives
	
Drive Site Traffic, Leads, and IOs

Increase brand awareness (YouGov)

Collect 14,399 IOs, Drive 20% of IOs from secondary markets

Bring on as many small businesses as possible

Expected Impact

Drive traffic, leads, and IOs: Drive incremental site traffic, leads, and IOs. Understand magnitude of impact by market to understand ROI, and clarify areas of opportunities. 

Increase Brand awareness: Grow awareness of brand, compare YoY growth based on YouGov data. 



- kick_off.ipynb: will iterate through each city and run the clean_auto_Causual_Impact_IO.ipynb

- clean_auto_Causual_Impact_IO.ipynb: will have all the causal impact logic
    - Pulls the data
    - Runs Causal Impact model
    - Saves Results

- concat_files.ipynb will concatenate all the output into one file
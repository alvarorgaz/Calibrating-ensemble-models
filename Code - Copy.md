---
output: rmarkdown::github_document
---

# 1. INSTALLATION AND CONNECTION TO SPARK

Notes:

- **N1.** As the package *SparkR* is removed from R CRAN, download the package file *SparkR_2.3.0.tar.gz* at the link: https://cran.r-project.org/src/contrib/Archive/SparkR/

- **N2.** You will need to have installed the software Java and the firewall unblocked.

- **N3.** The function *sparkR.init* of the package *SparkR* will install the latest version of *Spark* in your computer (if not installed), then it is not necessary that you do it manually. It will be used too by the function *spark_connect* of the package *sparklyr*.

Mute warnings
```{r}
options(warn=-1)
```

Install the necessary R packages from R CRAN (if not installed): *dplyr*, *sparklyr*, *pROC*, *DBI*, *ggplot2*
```{r}
install.packages("dplyr")
install.packages("sparklyr")
install.packages("pROC")
install.packages("DBI")
install.packages("ggplot2")
```

Install the necessary R package removed from R CRAN (if not installed) : *SparkR*
```{r}
install.packages("SparkR_2.3.0.tar.gz",repos=NULL,type="source")
```

Load the necessary installed packages
```{r}
library(SparkR)
library(dplyr)
library(sparklyr)
library(pROC)
library(DBI)
library(ggplot2)
```

Connect to *Spark* cluster in local mode (package *SparkR*)
```{r}
sc_SparkR <- sparkR.init(master="local")
sc_SparkR_sql <- sparkRSQL.init(sc_SparkR)
```

Connect to *Spark* cluster in local mode (package *sparklyr*)
```{r}
sc_sparklyr <- spark_connect(master="local")
```

Notes:

- **N4.** You can connect to both local instances of *Spark* as well as remote *Spark* clusters but we will connect to a local. The returned *Spark* connection (sc) provides a remote data source to the *Spark* cluster. Once you have connected to *Spark*, you will be able to browse the tables contained in the *Spark* cluster and also, in the case of the package *sparklyr*, preview *Spark* data frames using the RStudio data viewer.

# 2. DATA LOADING

Notes:

- **N5.** You can read and write data in CSV, JSON, and Parquet formats. Data can be stored in remote clusters or on the local cluster, and it returns a reference to a *Spark* data frame.

Load the data specifying the type of variables: numerical (*double*) or categorical (*character*)
```{r}
data <- spark_read_csv(sc=sc_sparklyr,
                       name="data",
                       path="Data Financed Defaults.csv",
                       header=TRUE,
                       delimiter=";",
                       infer_schema=FALSE,
                       columns=list(Contract_ID="character",
                                    Defaulted="character",
                                    Application_Hour_Group="character",
                                    Application_Week_Day="character",
                                    Amount="double",
                                    Maturity="double",
                                    Purpose="character",
                                    Province="character",
                                    Postal_Code_ASNEF="double",
                                    Age="double",
                                    Gender="character",
                                    Profession_Code="character",
                                    Profession_Sector="character",
                                    Contract_Type="character",
                                    Seniority="double",
                                    Housing_Type="character",
                                    Housing_Seniority="double",
                                    Marital_Status="character",
                                    People_in_Household="character",
                                    Income="double",
                                    Additional_Income="double",
                                    Partner_Income="double",
                                    Rent="double",
                                    Mortgage="double",
                                    Amount_of_Ongoing_Credits="double",
                                    Num_Ongoing_Credits="character"))
```

Number of customers and variables
```{r}
count(data)  # 3468 customers
ncol(data)   # 26 variables (including the identifier of customers)
```

# 3. ANALYSIS OF THE DATA VARIABLES BEFORE PRE-PROCESSING

Balanced or unbalanced dataset?
```{r}
count(filter(data,Defaulted==1))  # 705 = 20.33%
count(filter(data,Defaulted==0))  # 2763 = 79.67% 
```

Variables by type
```{r}
variables_type <- sdf_schema(data)
variables_type <- data.frame(Variable=names(variables_type),
                             Type=as.vector(unlist(sapply(names(variables_type),
                                                          function(i){variables_type[[i]][2]}))))
categorical <- variables_type[variables_type$Type=="StringType","Variable"]
numerical <- variables_type[variables_type$Type=="DoubleType","Variable"]
variables_type
```

Missing values by variables
```{r}
missings <- collect(data %>% mutate_all(is.na) %>% mutate_all(as.numeric) %>% summarize_all(sum))
missings <- data.frame(Variable=names(missings),Number_of_missings=as.vector(t(missings)))
missings[missings$Number_of_missings>0,]
```

Categories with frequency distribution <5% in its variable
```{r}
for(i in categorical[-which(categorical=="Contract_ID")]){
  show(dbGetQuery(sc_sparklyr,paste0("SELECT ",i,",COUNT(*)/3468 AS Distribution FROM data GROUP BY ",
                                     i," ORDER BY Distribution DESC")))
}
```

# 4. DATA PRE-PROCESSING

Modify missing values found in the analysis:

- in numerical variables by 0

- in categorical variables by *Missing*

```{r}
data <- data %>% mutate(
  Postal_Code_ASNEF=ifelse(is.na(Postal_Code_ASNEF),0,Postal_Code_ASNEF),
  Additional_Income=ifelse(is.na(Additional_Income),0,Additional_Income),
  Partner_Income=ifelse(is.na(Partner_Income),0,Partner_Income),
  Rent=ifelse(is.na(Rent),0,Rent),
  Mortgage=ifelse(is.na(Mortgage),0,Mortgage),
  Profession_Code=ifelse(is.na(Profession_Code),"Missing",Profession_Code)
  )
```

Create the list with valid levels for categorical variables (excluding categories with <5%)
```{r}
valid_levels <- list(
  Levels_Application_Hour_Group=
    c("[23H, 7H)","[7H, 20H)","[20H, 23H)","OTHERS"),
  Levels_Application_Week_Day=
    c("1","2","3","4","5","6","7","OTHERS"),
  Levels_Gender=
    c("MALE","FEMALE","OTHERS"),
  Levels_Profession_Sector=
    c("PRIVATE_SECTOR","PUBLIC_SECTOR","OTHERS"),
  Levels_Contract_Type=
    c("PERMANENT","PENSION","OTHERS"),
  Levels_People_in_Household=
    c("0","1","2","OTHERS"),
  Levels_Num_Ongoing_Credits=
    c("0","1","2","3","OTHERS"),
  Levels_Marital_Status=
    c("DIVORCED","SINGLE","COHABITING","MARRIED","OTHERS"),
  Levels_Province=
    c("Madrid","Barcelona","Asturias (Oviedo)","OTHERS"),
  Levels_Profession_Code=
    c("OPERATOR", "ADMINISTRATIVE","TECHNICIAN","MIDDLEGRADEMANAGER","RETIREMENT","OTHERS"),
  Levels_Purpose=
    c("LIQUIDITY","HOMEIMPROVEMENT","DEBTS","FURNITURE_AND_APPLIANCES","USEDCAR","MEDICALCARE",
      "VACATION","OTHERS"),
  Levels_Housing_Type=
    c("THIRD_PARTY_PROVIDED_LODGING","HOME_OWNERSHIP_WITHOUT_MORTGAGE","TENANT",
      "HOME_OWNERSHIP_WITH_MORTGAGE","OTHERS")
  )
```

Change outliers categories by *OTHERS*
```{r}
data <- data %>% mutate(
  Purpose=
    ifelse(Purpose %in% valid_levels[["Levels_Purpose"]],Purpose,"OTHERS"),
  Gender=
    ifelse(Gender %in% valid_levels[["Levels_Gender"]],Gender,"OTHERS"),
  Housing_Type=
    ifelse(Housing_Type %in% valid_levels[["Levels_Housing_Type"]],Housing_Type,"OTHERS"),
  Province=
    ifelse(Province %in% valid_levels[["Levels_Province"]],Province,"OTHERS"),
  Marital_Status=
    ifelse(Marital_Status %in% valid_levels[["Levels_Marital_Status"]],Marital_Status,"OTHERS"),
  Profession_Code=
    ifelse(Profession_Code %in% valid_levels[["Levels_Profession_Code"]],Profession_Code,"OTHERS"),
  Contract_Type=
    ifelse(Contract_Type %in% valid_levels[["Levels_Contract_Type"]],Contract_Type,"OTHERS"),
  Profession_Sector=
    ifelse(Profession_Sector %in% valid_levels[["Levels_Profession_Sector"]],Profession_Sector,
           "OTHERS"),
  Application_Week_Day=
    ifelse(Application_Week_Day %in% valid_levels[["Levels_Application_Week_Day"]],
           Application_Week_Day,"OTHERS"),
  People_in_Household=
    ifelse(People_in_Household %in% valid_levels[["Levels_People_in_Household"]],People_in_Household,
           "OTHERS"),
  Num_Ongoing_Credits=
    ifelse(Num_Ongoing_Credits %in% valid_levels[["Levels_Num_Ongoing_Credits"]],Num_Ongoing_Credits,
           "OTHERS"),
  Application_Hour_Group=
    ifelse(Application_Hour_Group %in% valid_levels[["Levels_Application_Hour_Group"]],
           Application_Hour_Group,"OTHERS")
  )
```

Notes:

- **N6.** In the case that you need to convert all categorical variables to numerical with an integer index, the necessary *Spark R* code could be:

```{r}
for(i in categorical[-which(categorical=="Contract_ID" | categorical=="Defaulted")]){
  label_i  <- as.vector(valid_levels[[paste0("Levels_",i)]])
  data <- data %>% ft_string_indexer_model(input_col=i,output_col=paste0(i,"_INDEXED"),labels=label_i)
}
```

Save the *Spark* data frame with the pre-processed data in the *Spark* cluster
```{r}
data <- copy_to(sc_sparklyr,data,overwrite=T)
```

Save the *Spark* data frame with the pre-processed data in a local file with format Parquet
```{r}
spark_write_parquet(data,"data.parquet")
```

# 5. PROTOCOL OF MODEL VALIDATION PHASE 1: Create training and test sets as well as training folds for CV

Read the *Spark* data frame with the pre-processed data in a local file with format Parquet
```{r}
data <- spark_read_parquet(sc_sparklyr,"data","data.parquet")
```

Create training and test datasets (75%-25%)
```{r}
data_partitions <- data %>% compute("data_partitions") %>% sdf_partition(train=0.75,test=0.25,seed=1)
```

Create K folds or partitions from training data for cross-validation
```{r}
K <- 5
weights <- rep(1/K,times = K)
names(weights) <- paste0("Fold ",as.character(1:K))
train_partitions <- data_partitions$train %>% compute("train_partitions") %>% 
  sdf_partition(weights=weights,seed=1) 
```

Check that all categories in the test set are included in the training set
```{r}
for(i in categorical[-which(categorical=="Contract_ID")]){
  cat("Does the variable ",i," have the same categories in training and test sets?","\n")
  cat(sum(!unique(as.data.frame(collect(data_partitions$test))[,i]) %in% 
           unique(as.data.frame(collect(data_partitions$train))[,i]))==0,"\n")
}
```

Notes:

- **N7.** The function *sdf_partition* returns a list with as much *Spark* datasets as you define. The fold weights are the probabilities of being in every fold for the observations, and they do not mean the fold size.

# 6. PROTOCOL OF MODEL VALIDATION PHASE 2: Find the best parametrization of every model with training set

Create the vectors with the names of variables by types
```{r}
out <- c("Contract_ID","Postal_Code_ASNEF","Additional_Income","Partner_Income","Rent","Mortgage")
response <- c("Defaulted")
features_num <- c("Amount","Maturity","Postal_Code_ASNEF","Age","Seniority","Housing_Seniority",
                  "Income","Additional_Income","Rent","Partner_Income","Mortgage",
                  "Amount_of_Ongoing_Credits")
features_cat <- c("Application_Hour_Group","Application_Week_Day","Purpose","Province","Gender",
                  "Profession_Code","Profession_Sector","Contract_Type","Housing_Type",
                  "Marital_Status","People_in_Household","Num_Ongoing_Credits")
```

## 6.1 LOGISTIC REGRESSION

Starting time
```{r}
Sys.time()
```

Create the list of candidate parametrizations
```{r}
params <- list(features_num,features_cat,c(features_num,features_cat))
```

Iterate all candidate parametrizations
```{r}
results_cv_LR <- data.frame()

for(p in 1:length(params)){
  
  response_LR_global <- c()
  pred_LR_global <- c()

  # Iterate all training folds
  for(i in 1:K){
  
    # Create the training and test sets for the ith fold
    test_i <- train_partitions[[i]]
    train_i <- rbind(train_partitions[[c(1:K)[-i][1]]],train_partitions[[c(1:K)[-i][2]]],
                     train_partitions[[c(1:K)[-i][3]]],train_partitions[[c(1:K)[-i][4]]])
    
    # Train the model without ith fold and the pth parametrization
    model_LR_i <- ml_logistic_regression(train_i,
                                         response=response,
                                         features=params[[p]])
    
    # Predict ith fold with the pth parametrization
    pred_LR_i <- sdf_predict(test_i,model_LR_i)
        pred_LR_i <- data.frame(collect(pred_LR_i %>% select(probability_1)))[,"probability_1"]
    
    # Calculate the OOB AUC for the ith fold with the pth parametrization
    response_i <- data.frame(collect(test_i %>% select(Defaulted)))[,"Defaulted"]
    results_cv_LR[p,i] <- auc(roc(response_i,pred_LR_i))
    
    # Save the ith fold response and predictions with the pth parametrization
    response_LR_global <- c(response_LR_global,response_i)
    pred_LR_global <- c(pred_LR_global,pred_LR_i)
  }
  
  # Calculate the rest of performance measures with the global training set predicted as OOB
  quartile_cutoff <- quantile(pred_LR_global,seq(0.25,0.75,0.25))
  quartile <- ifelse(pred_LR_global<=quartile_cutoff[1],"Q1",
                        ifelse(pred_LR_global<=quartile_cutoff[2],"Q2",
                               ifelse(pred_LR_global<=quartile_cutoff[3],"Q3","Q4")))
  results_cv_LR[p,6] <- auc(roc(response_LR_global,pred_LR_global))
  results_cv_LR[p,7] <- mean(response_LR_global[quartile=="Q1"]==1)
  results_cv_LR[p,8] <- mean(response_LR_global[quartile=="Q2"]==1)
  results_cv_LR[p,9] <- mean(response_LR_global[quartile=="Q3"]==1)
  results_cv_LR[p,10] <- mean(response_LR_global[quartile=="Q4"]==1)
}
```

Print the results table
```{r}
row.names(results_cv_LR) <- c("Parametrization 1: numerical features",
                              "Parametrization 2: categorical features",
                              "Parametrization 3: all features")
names(results_cv_LR) <- c("AUC Fold 1","AUC Fold 2","AUC Fold 3","AUC Fold 4","AUC Fold 5",
                          "AUC Global","% True + Q1","% True + Q2","% True + Q3","% True + Q4")
save(results_cv_LR,file="results_cv_LR.RData")
results_cv_LR
```

Finishing time
```{r}
Sys.time()
```

## 6.2 DECISION TREE

Starting time
```{r}
Sys.time()
```

Create the list of candidate parametrizations
```{r}
max_bins <- c(10,20,30)
max_depth <- c(5,10,15)
min_instances_per_node <- c(5,9,13)
params <- expand.grid(max_bins,max_depth,min_instances_per_node)
params <- sapply(1:nrow(params),function(i){list(params[i,])})
```

Iterate all candidate parametrizations
```{r}
results_cv_DT <- data.frame()

for(p in 1:length(params)){

  response_DT_global <- c()
  pred_DT_global <- c()
  
  # Iterate all training folds
  for(i in 1:K){
    
    # Create the training and test sets for the ith fold
    test_i <- train_partitions[[i]]
    train_i <- rbind(train_partitions[[c(1:K)[-i][1]]],train_partitions[[c(1:K)[-i][2]]],
                     train_partitions[[c(1:K)[-i][3]]],train_partitions[[c(1:K)[-i][4]]])
    
    # Train the model without ith fold and the pth parametrization
    model_DT_i <- ml_decision_tree(train_i,
                                   type="classification",
                                   response=response,
                                   features=c(features_num,features_cat),
                                   max_bins=as.numeric(params[[p]][1]),
                                   max_depth=as.numeric(params[[p]][2]),
                                   min_instances_per_node=as.numeric(params[[p]][3]),
                                   seed=1)
    
    # Predict ith fold with the pth parametrization
    pred_DT_i <- sdf_predict(test_i,model_DT_i)
    pred_DT_i <- data.frame(collect(pred_DT_i %>% select(probability_1)))[,"probability_1"]
    
    # Calculate the OOB AUC for the ith fold with the pth parametrization
    response_i <- data.frame(collect(test_i %>% select(Defaulted)))[,"Defaulted"]
    results_cv_DT[p,i] <- auc(roc(response_i,pred_DT_i))
    
    # Save the ith fold response and predictions with the pth parametrization
    response_DT_global <- c(response_DT_global,response_i)
    pred_DT_global <- c(pred_DT_global,pred_DT_i)
  }
  
  # Calculate the rest of performance measures with the global training set predicted as OOB
  quartile_cutoff <- quantile(pred_DT_global,seq(0.25,0.75,0.25))
  quartile <- ifelse(pred_DT_global<=quartile_cutoff[1],"Q1",
                     ifelse(pred_DT_global<=quartile_cutoff[2],"Q2",
                            ifelse(pred_DT_global<=quartile_cutoff[3],"Q3","Q4")))
  results_cv_DT[p,6] <- auc(roc(response_DT_global,pred_DT_global))
  results_cv_DT[p,7] <- mean(response_DT_global[quartile=="Q1"]==1)
  results_cv_DT[p,8] <- mean(response_DT_global[quartile=="Q2"]==1)
  results_cv_DT[p,9] <- mean(response_DT_global[quartile=="Q3"]==1)
  results_cv_DT[p,10] <- mean(response_DT_global[quartile=="Q4"]==1)
}
```

Print the results table
```{r}
row.names(results_cv_DT) <- sapply(1:length(params),function(i){
  paste0("max_bins=",params[[i]][1],";max_depth=",params[[i]][2],";min_instances_node=",
         params[[i]][3])})
names(results_cv_DT) <- c("AUC Fold 1","AUC Fold 2","AUC Fold 3","AUC Fold 4","AUC Fold 5",
                          "AUC Global","% True + Q1","% True + Q2","% True + Q3","% True + Q4")
save(results_cv_DT,file="results_cv_DT.RData")
results_cv_DT
```

Finishing time
```{r}
Sys.time()
```

## 6.3 RANDOM FOREST

Starting time
```{r}
Sys.time()
```

Create the list of candidate parametrizations
```{r}
max_bins <- c(10,20,30)
max_depth <- c(5,10,15)
num.trees <- c(15,30,45)
min_instances_per_node <- c(5,9,13)
params <- expand.grid(max_bins,max_depth,num.trees,min_instances_per_node)
params <- sapply(1:nrow(params),function(i){list(params[i,])})
```

Iterate all candidate parametrizations
```{r}
results_cv_RF <- data.frame()

for(p in 1:length(params)){
  
  response_RF_global <- c()
  pred_RF_global <- c()
  
  # Iterate all training folds
  for(i in 1:K){
    
    # Create the training and test sets for the ith fold
    test_i <- train_partitions[[i]]
    train_i <- rbind(train_partitions[[c(1:K)[-i][1]]],train_partitions[[c(1:K)[-i][2]]],
                     train_partitions[[c(1:K)[-i][3]]],train_partitions[[c(1:K)[-i][4]]])
    
    # Train the model without ith fold and the pth parametrization
    model_RF_i <- ml_random_forest(train_i,
                                   type="classification",
                                   response=response,
                                   features=c(features_num,features_cat),
                                   max_bins=as.numeric(params[[p]][1]),
                                   max_depth=as.numeric(params[[p]][2]),
                                   num_trees=as.numeric(params[[p]][3]),
                                   min_instances_per_node=as.numeric(params[[p]][4]),
                                   subsampling_rate=1,
                                   seed=1)
    
    # Predict ith fold with the pth parametrization
    pred_RF_i <- sdf_predict(test_i,model_RF_i)
    pred_RF_i <- data.frame(collect(pred_RF_i %>% select(probability_1)))[,"probability_1"]
    
    # Calculate the OOB AUC for the ith fold with the pth parametrization
    response_i <- data.frame(collect(test_i %>% select(Defaulted)))[,"Defaulted"]
    results_cv_RF[p,i] <- auc(roc(response_i,pred_RF_i))
    
    # Save the ith fold response and predictions with the pth parametrization
    response_RF_global <- c(response_RF_global,response_i)
    pred_RF_global <- c(pred_RF_global,pred_RF_i)
  }
  
  # Calculate the rest of performance measures with the global training set predicted as OOB
  quartile_cutoff <- quantile(pred_RF_global,seq(0.25,0.75,0.25))
  quartile <- ifelse(pred_RF_global<=quartile_cutoff[1],"Q1",
                     ifelse(pred_RF_global<=quartile_cutoff[2],"Q2",
                            ifelse(pred_RF_global<=quartile_cutoff[3],"Q3","Q4")))
  results_cv_RF[p,6] <- auc(roc(response_RF_global,pred_RF_global))
  results_cv_RF[p,7] <- mean(response_RF_global[quartile=="Q1"]==1)
  results_cv_RF[p,8] <- mean(response_RF_global[quartile=="Q2"]==1)
  results_cv_RF[p,9] <- mean(response_RF_global[quartile=="Q3"]==1)
  results_cv_RF[p,10] <- mean(response_RF_global[quartile=="Q4"]==1)
}
```

Print the results table 
```{r}
row.names(results_cv_RF) <- sapply(1:length(params),function(i){
  paste0("max_bins=",params[[i]][1],";max_depth=",params[[i]][2],";num_trees=",params[[i]][3],
         ";min_instances_node=",params[[i]][4])})
names(results_cv_RF) <- c("AUC Fold 1","AUC Fold 2","AUC Fold 3","AUC Fold 4","AUC Fold 5",
                          "AUC Global","% True + Q1","% True + Q2","% True + Q3","% True + Q4")
save(results_cv_RF,file="results_cv_RF.RData")
results_cv_RF
```

Finishing time
```{r}
Sys.time()
```

## 6.4 GRADIENT BOOSTED TREES

Starting time
```{r}
Sys.time()
```

Create the list of candidate parametrizations
```{r}
max_depth <- c(5,10,15)
max_iter <- c(15,30,45)
step_size <- c(0.05,0.1,0.15)
params <- expand.grid(max_depth,max_iter,step_size)
params <- sapply(1:nrow(params),function(i){list(params[i,])})
```

Iterate all candidate parametrizations
```{r}
results_cv_GBT <- data.frame()

for(p in 1:length(params)){
  
  response_GBT_global <- c()
  pred_GBT_global <- c()
  
  # Iterate all training folds
  for(i in 1:K){
    
    # Create the training and test sets for the ith fold
    test_i <- train_partitions[[i]]
    train_i <- rbind(train_partitions[[c(1:K)[-i][1]]],train_partitions[[c(1:K)[-i][2]]],
                     train_partitions[[c(1:K)[-i][3]]],train_partitions[[c(1:K)[-i][4]]])
    
    # Create the training and test sets for the ith fold in package "SparkR" format
    test_i_SparkR <- createDataFrame(sc_SparkR_sql,as.data.frame(collect(test_i)))
    train_i_SparkR <- createDataFrame(sc_SparkR_sql,as.data.frame(collect(train_i)))
    
    # Train the model without ith fold and the pth parametrization
    formula <- as.formula(paste(response,"~",paste(c(features_num,features_cat),collapse="+")))
    model_GBT_i <- spark.gbt(train_i_SparkR,
                             formula=formula,
                             type="classification",
                             maxDepth=as.numeric(params[[p]][1]),
                             maxIter=as.numeric(params[[p]][2]),
                             stepSize=as.numeric(params[[p]][3]),
                             subsamplingRate=1,
                             seed=1)
    
    # Predict ith fold with the pth parametrization
    pred_GBT_i <- predict(model_GBT_i,test_i_SparkR)
    pred_GBT_i <- unlist(lapply(as.data.frame(pred_GBT_i)[,"probability"],
                                function(x)SparkR:::callJMethod(x,"toArray")[[2]]))
    
    # Calculate the OOB AUC for the ith fold with the pth parametrization
    response_i <- data.frame(collect(test_i %>% select(Defaulted)))[,"Defaulted"]
    results_cv_GBT[p,i] <- auc(roc(response_i,pred_GBT_i))
    
    # Save the ith fold response and predictions with the pth parametrization
    response_GBT_global <- c(response_GBT_global,response_i)
    pred_GBT_global <- c(pred_GBT_global,pred_GBT_i)
  }
 
  # Calculate the rest of performance measures with the global training set predicted as OOB
  quartile_cutoff <- quantile(pred_GBT_global,seq(0.25,0.75,0.25))
  quartile <- ifelse(pred_GBT_global<=quartile_cutoff[1],"Q1",
                     ifelse(pred_GBT_global<=quartile_cutoff[2],"Q2",
                            ifelse(pred_GBT_global<=quartile_cutoff[3],"Q3","Q4")))
  results_cv_GBT[p,6] <- auc(roc(response_GBT_global,pred_GBT_global))
  results_cv_GBT[p,7] <- mean(response_GBT_global[quartile=="Q1"]==1)
  results_cv_GBT[p,8] <- mean(response_GBT_global[quartile=="Q2"]==1)
  results_cv_GBT[p,9] <- mean(response_GBT_global[quartile=="Q3"]==1)
  results_cv_GBT[p,10] <- mean(response_GBT_global[quartile=="Q4"]==1)
}
```

Print the results table
```{r}
row.names(results_cv_GBT) <- sapply(1:length(params),function(i){
  paste0("max_depth=",params[[i]][1],";max_iter=",params[[i]][2],
         ";step_size=",params[[i]][3])})
names(results_cv_GBT) <- c("AUC Fold 1","AUC Fold 2","AUC Fold 3","AUC Fold 4","AUC Fold 5",
                          "AUC Global","% True + Q1","% True + Q2","% True + Q3","% True + Q4")
save(results_cv_GBT,file="results_cv_GBT.RData")
results_cv_GBT
```

Finishing time
```{r}
Sys.time()
```

## 6.5 NAIVE BAYES

Starting time
```{r}
Sys.time()
```

Create the list of candidate parametrizations
```{r}
params <- list(features_num,features_cat,c(features_num,features_cat))
```

Iterate all candidate parametrizations
```{r}
results_cv_NB <- data.frame()

for(p in 1:length(params)){
  
  response_NB_global <- c()
  pred_NB_global <- c()
  
  # Iterate all training folds
  for(i in 1:K){
    
    # Create the training and test sets for the ith fold
    test_i <- train_partitions[[i]]
    train_i <- rbind(train_partitions[[c(1:K)[-i][1]]],train_partitions[[c(1:K)[-i][2]]],
                     train_partitions[[c(1:K)[-i][3]]],train_partitions[[c(1:K)[-i][4]]])
    
    # Train the model without ith fold and the pth parametrization
    model_NB_i <- ml_naive_bayes(train_i,
                                 response=response,
                                 features=params[[p]])
    
    # Predict ith fold with the pth parametrization
    pred_NB_i <- sdf_predict(test_i,model_NB_i)
    pred_NB_i <- data.frame(collect(pred_NB_i %>% select(probability_1)))[,"probability_1"]
    
    # Calculate the OOB AUC for the ith fold with the pth parametrization
    response_i <- data.frame(collect(test_i %>% select(Defaulted)))[,"Defaulted"]
    results_cv_NB[p,i] <- auc(roc(response_i,pred_NB_i))
    
    # Save the ith fold response and predictions with the pth parametrization
    response_NB_global <- c(response_NB_global,response_i)
    pred_NB_global <- c(pred_NB_global,pred_NB_i)
  }
  
  # Calculate the rest of performance measures with the global training set predicted as OOB
  quartile_cutoff <- quantile(pred_NB_global,seq(0.25,0.75,0.25))
  quartile <- ifelse(pred_NB_global<=quartile_cutoff[1],"Q1",
                     ifelse(pred_NB_global<=quartile_cutoff[2],"Q2",
                            ifelse(pred_NB_global<=quartile_cutoff[3],"Q3","Q4")))
  results_cv_NB[p,6] <- auc(roc(response_NB_global,pred_NB_global))
  results_cv_NB[p,7] <- mean(response_NB_global[quartile=="Q1"]==1)
  results_cv_NB[p,8] <- mean(response_NB_global[quartile=="Q2"]==1)
  results_cv_NB[p,9] <- mean(response_NB_global[quartile=="Q3"]==1)
  results_cv_NB[p,10] <- mean(response_NB_global[quartile=="Q4"]==1)
}
```

Print the results table
```{r}
row.names(results_cv_NB) <- c("Parametrization 1: numerical features",
                              "Parametrization 2: categorical features",
                              "Parametrization 3: all features")
names(results_cv_NB) <- c("AUC Fold 1","AUC Fold 2","AUC Fold 3","AUC Fold 4","AUC Fold 5",
                          "AUC Global","% True + Q1","% True + Q2","% True + Q3","% True + Q4")
save(results_cv_NB,file="results_cv_NB.RData")
results_cv_NB
```

Finishing time
```{r}
Sys.time()
```

## 6.6 NEURAL NETWORK

Starting time
```{r}
Sys.time()
```

Create the list of candidate parametrizations
```{r}
layer_hidden_1 <- c(6,8,10,12)
layer_hidden_2 <- c(4,6,8,10)
params <- expand.grid(layer_hidden_1,layer_hidden_2)
params <- sapply(1:nrow(params),function(i){list(params[i,])})
```

Iterate all candidate parametrizations
```{r}
results_cv_NN <- data.frame()

for(p in 1:length(params)){
  
  response_NN_global <- c()
  pred_NN_global <- c()
  
  # Iterate all training folds
  for(i in 1:K){
    
    # Create the training and test sets for the ith fold
    test_i <- train_partitions[[i]]
    train_i <- rbind(train_partitions[[c(1:K)[-i][1]]],train_partitions[[c(1:K)[-i][2]]],
                     train_partitions[[c(1:K)[-i][3]]],train_partitions[[c(1:K)[-i][4]]])
    
    # Create the training and test sets for the ith fold in package "SparkR" format
    test_i_SparkR <- createDataFrame(sc_SparkR_sql,as.data.frame(collect(test_i)))
    train_i_SparkR <- createDataFrame(sc_SparkR_sql,as.data.frame(collect(train_i)))
    
    # Establish the number of neurons:
    layer_output <- 2
    layer_hidden_1 <- as.numeric(params[[p]][1])
    layer_hidden_2 <- as.numeric(params[[p]][2])
    layer_input <- sum(sapply(as.data.frame(train_i_SparkR[,features_cat]),
                              function(x)length(unique(x))-1),length(features_num))
    
    # Train the model without ith fold and the pth parametrization
    formula <- as.formula(paste(response,"~",paste(c(features_num,features_cat),collapse="+")))
    model_NN_i <- spark.mlp(train_i_SparkR,
                            formula=formula,
                            layers=c(layer_input,layer_hidden_1,layer_hidden_2,layer_output),
                            seed=1)
    
    # Predict ith fold with the pth parametrization
    pred_NN_i <- predict(model_NN_i,test_i_SparkR)
    pred_NN_i <- unlist(lapply(as.data.frame(pred_NN_i)[,"probability"],
                               function(x)SparkR:::callJMethod(x,"toArray")[[2]]))
   
    # Calculate the OOB AUC for the ith fold with the pth parametrization
    response_i <- data.frame(collect(test_i %>% select(Defaulted)))[,"Defaulted"]
    results_cv_NN[p,i] <- auc(roc(response_i,pred_NN_i))
    
    # Save the ith fold response and predictions with the pth parametrization
    response_NN_global <- c(response_NN_global,response_i)
    pred_NN_global <- c(pred_NN_global,pred_NN_i)
  }
  
  # Calculate the rest of performance measures with the global training set predicted as OOB
  quartile_cutoff <- quantile(pred_NN_global,seq(0.25,0.75,0.25))
  quartile <- ifelse(pred_NN_global<=quartile_cutoff[1],"Q1",
                     ifelse(pred_NN_global<=quartile_cutoff[2],"Q2",
                            ifelse(pred_NN_global<=quartile_cutoff[3],"Q3","Q4")))
  results_cv_NN[p,6] <- auc(roc(response_NN_global,pred_NN_global))
  results_cv_NN[p,7] <- mean(response_NN_global[quartile=="Q1"]==1)
  results_cv_NN[p,8] <- mean(response_NN_global[quartile=="Q2"]==1)
  results_cv_NN[p,9] <- mean(response_NN_global[quartile=="Q3"]==1)
  results_cv_NN[p,10] <- mean(response_NN_global[quartile=="Q4"]==1)
}
```

Print the results table
```{r}
row.names(results_cv_NN) <- sapply(1:length(params),function(i){
  paste0("hidden_layer_1=",params[[i]][1],";hidden_layer_2=",params[[i]][2])})
names(results_cv_NN) <- c("AUC Fold 1","AUC Fold 2","AUC Fold 3","AUC Fold 4","AUC Fold 5",
                          "AUC Global","% True + Q1","% True + Q2","% True + Q3","% True + Q4")
save(results_cv_NN,file="results_cv_NN.RData")
results_cv_NN
```

Finishing time
```{r}
Sys.time()
```

Notes:

- **N8.** In the Multilayer Perceptron Neural Network, the number of neurons by layers are:
    
    - *output layer:* the # of classes in the response.
    
    - *hidden layers:* tunning parameters.

    - *input layer:* as much nodes as input variables. It means the # of numerical variables, plus the # of unique categories in all categorical variables, less the # of categorical features (because the model creates, for every categorical feature, dummies for every category except one for avoiding linear dependence).
    
# 7. PROTOCOL OF MODEL VALIDATION PHASE 3: Train models with optimal parametrizations

Create target variable of training and test sets
```{r}
response_train <- data.frame(collect(data_partitions$train %>% select(Defaulted)))[,"Defaulted"]
response_test <- data.frame(collect(data_partitions$test %>% select(Defaulted)))[,"Defaulted"]
```

Create the training and test sets in *SparkR* package format
```{r}
train_SparkR <- createDataFrame(sc_SparkR_sql,as.data.frame(collect(data_partitions$train)))
test_SparkR <- createDataFrame(sc_SparkR_sql,as.data.frame(collect(data_partitions$test)))
```

Create the results table, one for training and one for test
```{r}
metrics_names <- c("Sensitivity + Specificity","Accuracy","AUC","% True + Q1","% True + Q2",
                   "% True + Q3","% True + Q4")
row.names <- c(paste0("Criteria: ",metrics_names),paste0("Cut-off: ",metrics_names[-3]))
results_optimals_train <- data.frame(row.names=row.names)
results_optimals_test <- data.frame(row.names=row.names)
```

# 7.1 LOGISTIC REGRESSION

Train the optimal model with all training set
```{r}
model_LR <- ml_logistic_regression(data_partitions$train,
                                   response=response,
                                   features=c(features_num,features_cat))
```

Summary of the model
```{r}
summary(model_LR)
```

Predict training data
```{r}
pred_LR_train <- sdf_predict(data_partitions$train,model_LR)
pred_LR_train <- data.frame(collect(pred_LR_train %>% select(probability_1)))[,"probability_1"]
```

Predict test data
```{r}
pred_LR_test <- sdf_predict(data_partitions$test,model_LR)
pred_LR_test <- data.frame(collect(pred_LR_test %>% select(probability_1)))[,"probability_1"]
```

Iterate all possible cut-offs and calculate performance measures in training set
```{r}
m1_LR_train <- c()
m2_LR_train <- c()
lowest <- trunc(min(pred_LR_train)*100)/100+0.01
highest <- trunc(max(pred_LR_train)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_LR_train_c <- table(Real=response_train,Predicted=ifelse(pred_LR_train>=c,"1","0"))
  m1_LR_train <- c(m1_LR_train,table_LR_train_c[2,2]/sum(table_LR_train_c[2,]) +
                               table_LR_train_c[1,1]/sum(table_LR_train_c[1,]))
  m2_LR_train <- c(m2_LR_train,sum(diag(table_LR_train_c))/sum(table_LR_train_c))
}

cutoffs_LR_train <- quantile(pred_LR_train,seq(0.25,1,0.25))
quartiles_LR_train <- ifelse(pred_LR_train<=cutoffs_LR_train[1],"Q1",
                             ifelse(pred_LR_train<=cutoffs_LR_train[2],"Q2",
                                    ifelse(pred_LR_train<=cutoffs_LR_train[3],"Q3","Q4")))

results_optimals_train[1,"LR"] <- max(m1_LR_train)
results_optimals_train[2,"LR"] <- max(m2_LR_train)
results_optimals_train[3,"LR"] <- auc(roc(response_train,pred_LR_train))
results_optimals_train[4,"LR"] <- mean(response_train[quartiles_LR_train=="Q1"]==1)
results_optimals_train[5,"LR"] <- mean(response_train[quartiles_LR_train=="Q2"]==1)
results_optimals_train[6,"LR"] <- mean(response_train[quartiles_LR_train=="Q3"]==1)
results_optimals_train[7,"LR"] <- mean(response_train[quartiles_LR_train=="Q4"]==1)
results_optimals_train[8,"LR"] <- seq(lowest,highest,0.01)[which(m1_LR_train==max(m1_LR_train))[1]]
results_optimals_train[9,"LR"] <- seq(lowest,highest,0.01)[which(m2_LR_train==max(m2_LR_train))[1]]
results_optimals_train[10,"LR"] <- cutoffs_LR_train[1]
results_optimals_train[11,"LR"] <- cutoffs_LR_train[2]
results_optimals_train[12,"LR"] <- cutoffs_LR_train[3]
results_optimals_train[13,"LR"] <- cutoffs_LR_train[4]
```

Iterate all possible cut-offs and calculate performance measures in test set
```{r}
m1_LR_test <- c()
m2_LR_test <- c()
lowest <- trunc(min(pred_LR_test)*100)/100+0.01
highest <- trunc(max(pred_LR_test)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_LR_test_c <- table(Real=response_test,Predicted=ifelse(pred_LR_test>=c,"1","0"))
  m1_LR_test <- c(m1_LR_test,table_LR_test_c[2,2]/sum(table_LR_test_c[2,]) +
                     table_LR_test_c[1,1]/sum(table_LR_test_c[1,]))
  m2_LR_test <- c(m2_LR_test,sum(diag(table_LR_test_c))/sum(table_LR_test_c))
}

cutoffs_LR_test <- quantile(pred_LR_test,seq(0.25,1,0.25))
quartiles_LR_test <- ifelse(pred_LR_test<=cutoffs_LR_test[1],"Q1",
                             ifelse(pred_LR_test<=cutoffs_LR_test[2],"Q2",
                                    ifelse(pred_LR_test<=cutoffs_LR_test[3],"Q3","Q4")))

results_optimals_test[1,"LR"] <- max(m1_LR_test)
results_optimals_test[2,"LR"] <- max(m2_LR_test)
results_optimals_test[3,"LR"] <- auc(roc(response_test,pred_LR_test))
results_optimals_test[4,"LR"] <- mean(response_test[quartiles_LR_test=="Q1"]==1)
results_optimals_test[5,"LR"] <- mean(response_test[quartiles_LR_test=="Q2"]==1)
results_optimals_test[6,"LR"] <- mean(response_test[quartiles_LR_test=="Q3"]==1)
results_optimals_test[7,"LR"] <- mean(response_test[quartiles_LR_test=="Q4"]==1)
results_optimals_test[8,"LR"] <- seq(lowest,highest,0.01)[which(m1_LR_test==max(m1_LR_test))[1]]
results_optimals_test[9,"LR"] <- seq(lowest,highest,0.01)[which(m2_LR_test==max(m2_LR_test))[1]]
results_optimals_test[10,"LR"] <- cutoffs_LR_test[1]
results_optimals_test[11,"LR"] <- cutoffs_LR_test[2]
results_optimals_test[12,"LR"] <- cutoffs_LR_test[3]
results_optimals_test[13,"LR"] <- cutoffs_LR_test[4]
```

# 7.2 DECISION TREE

Train the optimal model with all training set
```{r}
model_DT <- ml_decision_tree(data_partitions$train,
                             type="classification",
                             response=response,
                             features=c(features_num,features_cat),
                             max_bins=30,
                             max_depth=5,
                             min_instances_per_node=5,
                             seed=1)
```

Features importance
```{r}
ml_feature_importances(model_DT)
```

Predict training data
```{r}
pred_DT_train <- sdf_predict(data_partitions$train,model_DT)
pred_DT_train <- data.frame(collect(pred_DT_train %>% select(probability_1)))[,"probability_1"]
```

Predict test data
```{r}
pred_DT_test <- sdf_predict(data_partitions$test,model_DT)
pred_DT_test <- data.frame(collect(pred_DT_test %>% select(probability_1)))[,"probability_1"]
```
 
Iterate all possible cut-offs and calculate performance measures in training set
```{r}
m1_DT_train <- c()
m2_DT_train <- c()
lowest <- trunc(min(pred_DT_train)*100)/100+0.01
highest <- trunc(max(pred_DT_train)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_DT_train_c <- table(Real=response_train,Predicted=ifelse(pred_DT_train>=c,"1","0"))
  m1_DT_train <- c(m1_DT_train,table_DT_train_c[2,2]/sum(table_DT_train_c[2,]) +
                     table_DT_train_c[1,1]/sum(table_DT_train_c[1,]))
  m2_DT_train <- c(m2_DT_train,sum(diag(table_DT_train_c))/sum(table_DT_train_c))
}

cutoffs_DT_train <- quantile(pred_DT_train,seq(0.25,1,0.25))
quartiles_DT_train <- ifelse(pred_DT_train<=cutoffs_DT_train[1],"Q1",
                             ifelse(pred_DT_train<=cutoffs_DT_train[2],"Q2",
                                    ifelse(pred_DT_train<=cutoffs_DT_train[3],"Q3","Q4")))

results_optimals_train[1,"DT"] <- max(m1_DT_train)
results_optimals_train[2,"DT"] <- max(m2_DT_train)
results_optimals_train[3,"DT"] <- auc(roc(response_train,pred_DT_train))
results_optimals_train[4,"DT"] <- mean(response_train[quartiles_DT_train=="Q1"]==1)
results_optimals_train[5,"DT"] <- mean(response_train[quartiles_DT_train=="Q2"]==1)
results_optimals_train[6,"DT"] <- mean(response_train[quartiles_DT_train=="Q3"]==1)
results_optimals_train[7,"DT"] <- mean(response_train[quartiles_DT_train=="Q4"]==1)
results_optimals_train[8,"DT"] <- seq(lowest,highest,0.01)[which(m1_DT_train==max(m1_DT_train))[1]]
results_optimals_train[9,"DT"] <- seq(lowest,highest,0.01)[which(m2_DT_train==max(m2_DT_train))[1]]
results_optimals_train[10,"DT"] <- cutoffs_DT_train[1]
results_optimals_train[11,"DT"] <- cutoffs_DT_train[2]
results_optimals_train[12,"DT"] <- cutoffs_DT_train[3]
results_optimals_train[13,"DT"] <- cutoffs_DT_train[4]
```

Iterate all possible cut-offs and calculate performance measures in test set
```{r}
m1_DT_test <- c()
m2_DT_test <- c()
lowest <- trunc(min(pred_DT_test)*100)/100+0.01
highest <- trunc(max(pred_DT_test)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_DT_test_c <- table(Real=response_test,Predicted=ifelse(pred_DT_test>=c,"1","0"))
  m1_DT_test <- c(m1_DT_test,table_DT_test_c[2,2]/sum(table_DT_test_c[2,]) +
                    table_DT_test_c[1,1]/sum(table_DT_test_c[1,]))
  m2_DT_test <- c(m2_DT_test,sum(diag(table_DT_test_c))/sum(table_DT_test_c))
}

cutoffs_DT_test <- quantile(pred_DT_test,seq(0.25,1,0.25))
quartiles_DT_test <- ifelse(pred_DT_test<=cutoffs_DT_test[1],"Q1",
                            ifelse(pred_DT_test<=cutoffs_DT_test[2],"Q2",
                                   ifelse(pred_DT_test<=cutoffs_DT_test[3],"Q3","Q4")))

results_optimals_test[1,"DT"] <- max(m1_DT_test)
results_optimals_test[2,"DT"] <- max(m2_DT_test)
results_optimals_test[3,"DT"] <- auc(roc(response_test,pred_DT_test))
results_optimals_test[4,"DT"] <- mean(response_test[quartiles_DT_test=="Q1"]==1)
results_optimals_test[5,"DT"] <- mean(response_test[quartiles_DT_test=="Q2"]==1)
results_optimals_test[6,"DT"] <- mean(response_test[quartiles_DT_test=="Q3"]==1)
results_optimals_test[7,"DT"] <- mean(response_test[quartiles_DT_test=="Q4"]==1)
results_optimals_test[8,"DT"] <- seq(lowest,highest,0.01)[which(m1_DT_test==max(m1_DT_test))[1]]
results_optimals_test[9,"DT"] <- seq(lowest,highest,0.01)[which(m2_DT_test==max(m2_DT_test))[1]]
results_optimals_test[10,"DT"] <- cutoffs_DT_test[1]
results_optimals_test[11,"DT"] <- cutoffs_DT_test[2]
results_optimals_test[12,"DT"] <- cutoffs_DT_test[3]
results_optimals_test[13,"DT"] <- cutoffs_DT_test[4]
```

# 7.3 RANDOM FOREST

Train the optimal model with all training set
```{r}
model_RF <- ml_random_forest(data_partitions$train,
                             type="classification",
                             response=response,
                             features=c(features_num,features_cat),
                             max_bins=30,
                             max_depth=5,
                             num_trees=30,
                             min_instances_per_node=9,
                             subsampling_rate=1,
                             seed=1)
```

Features importance
```{r}
ml_feature_importances(model_GBT)
```

Predict training data
```{r}
pred_RF_train <- sdf_predict(data_partitions$train,model_RF)
pred_RF_train <- data.frame(collect(pred_RF_train %>% select(probability_1)))[,"probability_1"]
```

Predict test data
```{r}
pred_RF_test <- sdf_predict(data_partitions$test,model_RF)
pred_RF_test <- data.frame(collect(pred_RF_test %>% select(probability_1)))[,"probability_1"]
```

Iterate all possible cut-offs and calculate performance measures in training set
```{r}
m1_RF_train <- c()
m2_RF_train <- c()
lowest <- trunc(min(pred_RF_train)*100)/100+0.01
highest <- trunc(max(pred_RF_train)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_RF_train_c <- table(Real=response_train,Predicted=ifelse(pred_RF_train>=c,"1","0"))
  m1_RF_train <- c(m1_RF_train,table_RF_train_c[2,2]/sum(table_RF_train_c[2,]) +
                     table_RF_train_c[1,1]/sum(table_RF_train_c[1,]))
  m2_RF_train <- c(m2_RF_train,sum(diag(table_RF_train_c))/sum(table_RF_train_c))
}

cutoffs_RF_train <- quantile(pred_RF_train,seq(0.25,1,0.25))
quartiles_RF_train <- ifelse(pred_RF_train<=cutoffs_RF_train[1],"Q1",
                             ifelse(pred_RF_train<=cutoffs_RF_train[2],"Q2",
                                    ifelse(pred_RF_train<=cutoffs_RF_train[3],"Q3","Q4")))

results_optimals_train[1,"RF"] <- max(m1_RF_train)
results_optimals_train[2,"RF"] <- max(m2_RF_train)
results_optimals_train[3,"RF"] <- auc(roc(response_train,pred_RF_train))
results_optimals_train[4,"RF"] <- mean(response_train[quartiles_RF_train=="Q1"]==1)
results_optimals_train[5,"RF"] <- mean(response_train[quartiles_RF_train=="Q2"]==1)
results_optimals_train[6,"RF"] <- mean(response_train[quartiles_RF_train=="Q3"]==1)
results_optimals_train[7,"RF"] <- mean(response_train[quartiles_RF_train=="Q4"]==1)
results_optimals_train[8,"RF"] <- seq(lowest,highest,0.01)[which(m1_RF_train==max(m1_RF_train))[1]]
results_optimals_train[9,"RF"] <- seq(lowest,highest,0.01)[which(m2_RF_train==max(m2_RF_train))[1]]
results_optimals_train[10,"RF"] <- cutoffs_RF_train[1]
results_optimals_train[11,"RF"] <- cutoffs_RF_train[2]
results_optimals_train[12,"RF"] <- cutoffs_RF_train[3]
results_optimals_train[13,"RF"] <- cutoffs_RF_train[4]
```

Iterate all possible cut-offs and calculate performance measures in test set
```{r}
m1_RF_test <- c()
m2_RF_test <- c()
lowest <- trunc(min(pred_RF_test)*100)/100+0.01
highest <- trunc(max(pred_RF_test)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_RF_test_c <- table(Real=response_test,Predicted=ifelse(pred_RF_test>=c,"1","0"))
  m1_RF_test <- c(m1_RF_test,table_RF_test_c[2,2]/sum(table_RF_test_c[2,]) +
                    table_RF_test_c[1,1]/sum(table_RF_test_c[1,]))
  m2_RF_test <- c(m2_RF_test,sum(diag(table_RF_test_c))/sum(table_RF_test_c))
}

cutoffs_RF_test <- quantile(pred_RF_test,seq(0.25,1,0.25))
quartiles_RF_test <- ifelse(pred_RF_test<=cutoffs_RF_test[1],"Q1",
                            ifelse(pred_RF_test<=cutoffs_RF_test[2],"Q2",
                                   ifelse(pred_RF_test<=cutoffs_RF_test[3],"Q3","Q4")))

results_optimals_test[1,"RF"] <- max(m1_RF_test)
results_optimals_test[2,"RF"] <- max(m2_RF_test)
results_optimals_test[3,"RF"] <- auc(roc(response_test,pred_RF_test))
results_optimals_test[4,"RF"] <- mean(response_test[quartiles_RF_test=="Q1"]==1)
results_optimals_test[5,"RF"] <- mean(response_test[quartiles_RF_test=="Q2"]==1)
results_optimals_test[6,"RF"] <- mean(response_test[quartiles_RF_test=="Q3"]==1)
results_optimals_test[7,"RF"] <- mean(response_test[quartiles_RF_test=="Q4"]==1)
results_optimals_test[8,"RF"] <- seq(lowest,highest,0.01)[which(m1_RF_test==max(m1_RF_test))[1]]
results_optimals_test[9,"RF"] <- seq(lowest,highest,0.01)[which(m2_RF_test==max(m2_RF_test))[1]]
results_optimals_test[10,"RF"] <- cutoffs_RF_test[1]
results_optimals_test[11,"RF"] <- cutoffs_RF_test[2]
results_optimals_test[12,"RF"] <- cutoffs_RF_test[3]
results_optimals_test[13,"RF"] <- cutoffs_RF_test[4]
```

# 7.4  GRADIENT BOOSTED TREES

Train the optimal model with all training set
```{r}
formula <- as.formula(paste(response,"~",paste(c(features_num,features_cat),collapse="+")))
model_GBT <- spark.gbt(data=train_SparkR,
                       formula=formula,
                       type="classification",
                       maxDepth=10,
                       maxIter=15,
                       stepSize=0.1,
                       minInstancesPerNode=1,
                       seed=1)
```

Predict training data
```{r}
pred_GBT_train <- predict(model_GBT,train_SparkR)
pred_GBT_train <- unlist(lapply(as.data.frame(pred_GBT_train)[,"probability"],
                                function(x)SparkR:::callJMethod(x,"toArray")[[2]]))
```

Predict test data
```{r}
pred_GBT_test <- unlist(lapply(as.data.frame(pred_GBT_test)[,"probability"],
                               function(x)SparkR:::callJMethod(x,"toArray")[[2]]))
```

Iterate all possible cut-offs and calculate performance measures in training set
```{r}
m1_GBT_train <- c()
m2_GBT_train <- c()
lowest <- trunc(min(pred_GBT_train)*100)/100+0.01
highest <- trunc(max(pred_GBT_train)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_GBT_train_c <- table(Real=response_train,Predicted=ifelse(pred_GBT_train>=c,"1","0"))
  m1_GBT_train <- c(m1_GBT_train,table_GBT_train_c[2,2]/sum(table_GBT_train_c[2,]) +
                     table_GBT_train_c[1,1]/sum(table_GBT_train_c[1,]))
  m2_GBT_train <- c(m2_GBT_train,sum(diag(table_GBT_train_c))/sum(table_GBT_train_c))
}

cutoffs_GBT_train <- quantile(pred_GBT_train,seq(0.25,1,0.25))
quartiles_GBT_train <- ifelse(pred_GBT_train<=cutoffs_GBT_train[1],"Q1",
                             ifelse(pred_GBT_train<=cutoffs_GBT_train[2],"Q2",
                                    ifelse(pred_GBT_train<=cutoffs_GBT_train[3],"Q3","Q4")))

results_optimals_train[1,"GBT"] <- max(m1_GBT_train)
results_optimals_train[2,"GBT"] <- max(m2_GBT_train)
results_optimals_train[3,"GBT"] <- auc(roc(response_train,pred_GBT_train))
results_optimals_train[4,"GBT"] <- mean(response_train[quartiles_GBT_train=="Q1"]==1)
results_optimals_train[5,"GBT"] <- mean(response_train[quartiles_GBT_train=="Q2"]==1)
results_optimals_train[6,"GBT"] <- mean(response_train[quartiles_GBT_train=="Q3"]==1)
results_optimals_train[7,"GBT"] <- mean(response_train[quartiles_GBT_train=="Q4"]==1)
results_optimals_train[8,"GBT"] <- seq(lowest,highest,0.01)[which(m1_GBT_train==max(m1_GBT_train))[1]]
results_optimals_train[9,"GBT"] <- seq(lowest,highest,0.01)[which(m2_GBT_train==max(m2_GBT_train))[1]]
results_optimals_train[10,"GBT"] <- cutoffs_GBT_train[1]
results_optimals_train[11,"GBT"] <- cutoffs_GBT_train[2]
results_optimals_train[12,"GBT"] <- cutoffs_GBT_train[3]
results_optimals_train[13,"GBT"] <- cutoffs_GBT_train[4]
```

Iterate all possible cut-offs and calculate performance measures in test set
```{r}
m1_GBT_test <- c()
m2_GBT_test <- c()
lowest <- trunc(min(pred_GBT_test)*100)/100+0.01
highest <- trunc(max(pred_GBT_test)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_GBT_test_c <- table(Real=response_test,Predicted=ifelse(pred_GBT_test>=c,"1","0"))
  m1_GBT_test <- c(m1_GBT_test,table_GBT_test_c[2,2]/sum(table_GBT_test_c[2,]) +
                    table_GBT_test_c[1,1]/sum(table_GBT_test_c[1,]))
  m2_GBT_test <- c(m2_GBT_test,sum(diag(table_GBT_test_c))/sum(table_GBT_test_c))
}

cutoffs_GBT_test <- quantile(pred_GBT_test,seq(0.25,1,0.25))
quartiles_GBT_test <- ifelse(pred_GBT_test<=cutoffs_GBT_test[1],"Q1",
                            ifelse(pred_GBT_test<=cutoffs_GBT_test[2],"Q2",
                                   ifelse(pred_GBT_test<=cutoffs_GBT_test[3],"Q3","Q4")))

results_optimals_test[1,"GBT"] <- max(m1_GBT_test)
results_optimals_test[2,"GBT"] <- max(m2_GBT_test)
results_optimals_test[3,"GBT"] <- auc(roc(response_test,pred_GBT_test))
results_optimals_test[4,"GBT"] <- mean(response_test[quartiles_GBT_test=="Q1"]==1)
results_optimals_test[5,"GBT"] <- mean(response_test[quartiles_GBT_test=="Q2"]==1)
results_optimals_test[6,"GBT"] <- mean(response_test[quartiles_GBT_test=="Q3"]==1)
results_optimals_test[7,"GBT"] <- mean(response_test[quartiles_GBT_test=="Q4"]==1)
results_optimals_test[8,"GBT"] <- seq(lowest,highest,0.01)[which(m1_GBT_test==max(m1_GBT_test))[1]]
results_optimals_test[9,"GBT"] <- seq(lowest,highest,0.01)[which(m2_GBT_test==max(m2_GBT_test))[1]]
results_optimals_test[10,"GBT"] <- cutoffs_GBT_test[1]
results_optimals_test[11,"GBT"] <- cutoffs_GBT_test[2]
results_optimals_test[12,"GBT"] <- cutoffs_GBT_test[3]
results_optimals_test[13,"GBT"] <- cutoffs_GBT_test[4]
```

# 7.5 NAIVE BAYES

Train the optimal model with all training set
```{r}
model_NB <- ml_naive_bayes(data_partitions$train,
                           response=response,
                           features=features_cat)
```

Predict training data
```{r}
pred_NB_train <- sdf_predict(data_partitions$train,model_NB)
pred_NB_train <- data.frame(collect(pred_NB_train %>% select(probability_1)))[,"probability_1"]
```

Predict test data
```{r}
pred_NB_test <- sdf_predict(data_partitions$test,model_NB)
pred_NB_test <- data.frame(collect(pred_NB_test %>% select(probability_1)))[,"probability_1"]
```

Iterate all possible cut-offs and calculate performance measures in training set
```{r}
m1_NB_train <- c()
m2_NB_train <- c()
lowest <- trunc(min(pred_NB_train)*100)/100+0.01
highest <- trunc(max(pred_NB_train)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_NB_train_c <- table(Real=response_train,Predicted=ifelse(pred_NB_train>=c,"1","0"))
  m1_NB_train <- c(m1_NB_train,table_NB_train_c[2,2]/sum(table_NB_train_c[2,]) +
                     table_NB_train_c[1,1]/sum(table_NB_train_c[1,]))
  m2_NB_train <- c(m2_NB_train,sum(diag(table_NB_train_c))/sum(table_NB_train_c))
}

cutoffs_NB_train <- quantile(pred_NB_train,seq(0.25,1,0.25))
quartiles_NB_train <- ifelse(pred_NB_train<=cutoffs_NB_train[1],"Q1",
                             ifelse(pred_NB_train<=cutoffs_NB_train[2],"Q2",
                                    ifelse(pred_NB_train<=cutoffs_NB_train[3],"Q3","Q4")))

results_optimals_train[1,"NB"] <- max(m1_NB_train)
results_optimals_train[2,"NB"] <- max(m2_NB_train)
results_optimals_train[3,"NB"] <- auc(roc(response_train,pred_NB_train))
results_optimals_train[4,"NB"] <- mean(response_train[quartiles_NB_train=="Q1"]==1)
results_optimals_train[5,"NB"] <- mean(response_train[quartiles_NB_train=="Q2"]==1)
results_optimals_train[6,"NB"] <- mean(response_train[quartiles_NB_train=="Q3"]==1)
results_optimals_train[7,"NB"] <- mean(response_train[quartiles_NB_train=="Q4"]==1)
results_optimals_train[8,"NB"] <- seq(lowest,highest,0.01)[which(m1_NB_train==max(m1_NB_train))[1]]
results_optimals_train[9,"NB"] <- seq(lowest,highest,0.01)[which(m2_NB_train==max(m2_NB_train))[1]]
results_optimals_train[10,"NB"] <- cutoffs_NB_train[1]
results_optimals_train[11,"NB"] <- cutoffs_NB_train[2]
results_optimals_train[12,"NB"] <- cutoffs_NB_train[3]
results_optimals_train[13,"NB"] <- cutoffs_NB_train[4]
```

Iterate all possible cut-offs and calculate performance measures in test set
```{r}
m1_NB_test <- c()
m2_NB_test <- c()
lowest <- trunc(min(pred_NB_test)*100)/100+0.01
highest <- trunc(max(pred_NB_test)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_NB_test_c <- table(Real=response_test,Predicted=ifelse(pred_NB_test>=c,"1","0"))
  m1_NB_test <- c(m1_NB_test,table_NB_test_c[2,2]/sum(table_NB_test_c[2,]) +
                    table_NB_test_c[1,1]/sum(table_NB_test_c[1,]))
  m2_NB_test <- c(m2_NB_test,sum(diag(table_NB_test_c))/sum(table_NB_test_c))
}

cutoffs_NB_test <- quantile(pred_NB_test,seq(0.25,1,0.25))
quartiles_NB_test <- ifelse(pred_NB_test<=cutoffs_NB_test[1],"Q1",
                            ifelse(pred_NB_test<=cutoffs_NB_test[2],"Q2",
                                   ifelse(pred_NB_test<=cutoffs_NB_test[3],"Q3","Q4")))

results_optimals_test[1,"NB"] <- max(m1_NB_test)
results_optimals_test[2,"NB"] <- max(m2_NB_test)
results_optimals_test[3,"NB"] <- auc(roc(response_test,pred_NB_test))
results_optimals_test[4,"NB"] <- mean(response_test[quartiles_NB_test=="Q1"]==1)
results_optimals_test[5,"NB"] <- mean(response_test[quartiles_NB_test=="Q2"]==1)
results_optimals_test[6,"NB"] <- mean(response_test[quartiles_NB_test=="Q3"]==1)
results_optimals_test[7,"NB"] <- mean(response_test[quartiles_NB_test=="Q4"]==1)
results_optimals_test[8,"NB"] <- seq(lowest,highest,0.01)[which(m1_NB_test==max(m1_NB_test))[1]]
results_optimals_test[9,"NB"] <- seq(lowest,highest,0.01)[which(m2_NB_test==max(m2_NB_test))[1]]
results_optimals_test[10,"NB"] <- cutoffs_NB_test[1]
results_optimals_test[11,"NB"] <- cutoffs_NB_test[2]
results_optimals_test[12,"NB"] <- cutoffs_NB_test[3]
results_optimals_test[13,"NB"] <- cutoffs_NB_test[4]
```

# 7.6 NEURAL NETWORK

Train the optimal model with all training set
```{r}
layer_output <- 2
layer_hidden_1 <- 12
layer_hidden_2 <- 4
layer_input <- sum(sapply(as.data.frame(train_SparkR[,features_cat]),function(x)length(unique(x))-1),
                   length(features_num))
formula <- as.formula(paste(response,"~",paste(c(features_num,features_cat),collapse="+")))
model_NN <- spark.mlp(train_SparkR,
                      formula=formula,
                      layers=c(layer_input,layer_hidden_1,layer_hidden_2,layer_output),
                      seed=1)
```

Predict training data
```{r}
pred_NN_train <- predict(model_NN,train_SparkR)
pred_NN_train <- unlist(lapply(as.data.frame(pred_NN_train)[,"probability"],
                               function(x)SparkR:::callJMethod(x,"toArray")[[2]]))
```

Predict test data
```{r}
pred_NN_test <- predict(model_NN,test_SparkR)
pred_NN_test <- unlist(lapply(as.data.frame(pred_NN_test)[,"probability"],
                              function(x)SparkR:::callJMethod(x,"toArray")[[2]]))
```

Iterate all possible cut-offs and calculate performance measures in training set
```{r}
m1_NN_train <- c()
m2_NN_train <- c()
lowest <- trunc(min(pred_NN_train)*100)/100+0.01
highest <- trunc(max(pred_NN_train)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_NN_train_c <- table(Real=response_train,Predicted=ifelse(pred_NN_train>=c,"1","0"))
  m1_NN_train <- c(m1_NN_train,table_NN_train_c[2,2]/sum(table_NN_train_c[2,]) +
                     table_NN_train_c[1,1]/sum(table_NN_train_c[1,]))
  m2_NN_train <- c(m2_NN_train,sum(diag(table_NN_train_c))/sum(table_NN_train_c))
}

cutoffs_NN_train <- quantile(pred_NN_train,seq(0.25,1,0.25))
quartiles_NN_train <- ifelse(pred_NN_train<=cutoffs_NN_train[1],"Q1",
                             ifelse(pred_NN_train<=cutoffs_NN_train[2],"Q2",
                                    ifelse(pred_NN_train<=cutoffs_NN_train[3],"Q3","Q4")))

results_optimals_train[1,"NN"] <- max(m1_NN_train)
results_optimals_train[2,"NN"] <- max(m2_NN_train)
results_optimals_train[3,"NN"] <- auc(roc(response_train,pred_NN_train))
results_optimals_train[4,"NN"] <- mean(response_train[quartiles_NN_train=="Q1"]==1)
results_optimals_train[5,"NN"] <- mean(response_train[quartiles_NN_train=="Q2"]==1)
results_optimals_train[6,"NN"] <- mean(response_train[quartiles_NN_train=="Q3"]==1)
results_optimals_train[7,"NN"] <- mean(response_train[quartiles_NN_train=="Q4"]==1)
results_optimals_train[8,"NN"] <- seq(lowest,highest,0.01)[which(m1_NN_train==max(m1_NN_train))[1]]
results_optimals_train[9,"NN"] <- seq(lowest,highest,0.01)[which(m2_NN_train==max(m2_NN_train))[1]]
results_optimals_train[10,"NN"] <- cutoffs_NN_train[1]
results_optimals_train[11,"NN"] <- cutoffs_NN_train[2]
results_optimals_train[12,"NN"] <- cutoffs_NN_train[3]
results_optimals_train[13,"NN"] <- cutoffs_NN_train[4]
```

Iterate all possible cut-offs and calculate performance measures in test set
```{r}
m1_NN_test <- c()
m2_NN_test <- c()
lowest <- trunc(min(pred_NN_test)*100)/100+0.01
highest <- trunc(max(pred_NN_test)*100)/100

for(c in seq(lowest,highest,0.01)){
  table_NN_test_c <- table(Real=response_test,Predicted=ifelse(pred_NN_test>=c,"1","0"))
  m1_NN_test <- c(m1_NN_test,table_NN_test_c[2,2]/sum(table_NN_test_c[2,]) +
                    table_NN_test_c[1,1]/sum(table_NN_test_c[1,]))
  m2_NN_test <- c(m2_NN_test,sum(diag(table_NN_test_c))/sum(table_NN_test_c))
}

cutoffs_NN_test <- quantile(pred_NN_test,seq(0.25,1,0.25))
quartiles_NN_test <- ifelse(pred_NN_test<=cutoffs_NN_test[1],"Q1",
                            ifelse(pred_NN_test<=cutoffs_NN_test[2],"Q2",
                                   ifelse(pred_NN_test<=cutoffs_NN_test[3],"Q3","Q4")))

results_optimals_test[1,"NN"] <- max(m1_NN_test)
results_optimals_test[2,"NN"] <- max(m2_NN_test)
results_optimals_test[3,"NN"] <- auc(roc(response_test,pred_NN_test))
results_optimals_test[4,"NN"] <- mean(response_test[quartiles_NN_test=="Q1"]==1)
results_optimals_test[5,"NN"] <- mean(response_test[quartiles_NN_test=="Q2"]==1)
results_optimals_test[6,"NN"] <- mean(response_test[quartiles_NN_test=="Q3"]==1)
results_optimals_test[7,"NN"] <- mean(response_test[quartiles_NN_test=="Q4"]==1)
results_optimals_test[8,"NN"] <- seq(lowest,highest,0.01)[which(m1_NN_test==max(m1_NN_test))[1]]
results_optimals_test[9,"NN"] <- seq(lowest,highest,0.01)[which(m2_NN_test==max(m2_NN_test))[1]]
results_optimals_test[10,"NN"] <- cutoffs_NN_test[1]
results_optimals_test[11,"NN"] <- cutoffs_NN_test[2]
results_optimals_test[12,"NN"] <- cutoffs_NN_test[3]
results_optimals_test[13,"NN"] <- cutoffs_NN_test[4]
```

# 8. PROTOCOL OF MODEL VALIDATION PHASE 4: Compare the models with performance measures

Save and print the results table of the training set
```{r}
save(results_optimals_train,file="results_optimals_train.RData")
results_optimals_train
```

Save and print the results table of the test set
```{r}
save(results_optimals_test,file="results_optimals_test.RData")
results_optimals_test
```

# 8.1 PRINCIPAL COMPONENTS ANALYSIS

Merge the training and test sets
```{r}
data_pca <- rbind(data_partitions$train,data_partitions$test)
```

Normalize numerical variables for *PCA*
```{r}
data_pca <- data_pca %>% mutate(
  Amount=(Amount-mean(Amount))/sd(Amount),
  Maturity=(Maturity-mean(Maturity))/sd(Maturity),
  Postal_Code_ASNEF=(Postal_Code_ASNEF-mean(Postal_Code_ASNEF))/sd(Postal_Code_ASNEF),
  Age=(Age-mean(Age))/sd(Age),
  Seniority=(Seniority-mean(Seniority))/sd(Seniority),
  Housing_Seniority=(Housing_Seniority-mean(Housing_Seniority))/sd(Housing_Seniority),
  Income=(Income-mean(Income))/sd(Income),
  Additional_Income=(Additional_Income-mean(Additional_Income))/sd(Additional_Income),
  Rent=(Rent-mean(Rent))/sd(Rent),
  Partner_Income=(Partner_Income-mean(Partner_Income))/sd(Partner_Income),
  Mortgage=(Mortgage-mean(Mortgage))/sd(Mortgage),
  Amount_of_Ongoing_Credits=(Amount_of_Ongoing_Credits-mean(Amount_of_Ongoing_Credits))/sd(Amount_of_Ongoing_Credits))
```

Run *PCA* and project every observation into the new space
```{r}
pca <- data_pca %>% ml_pca(k=2,features=features_num)
pca_projections <- as.data.frame(pca %>% sdf_project() %>% select(PC1,PC2))
```

Plot the 2 main principal components
```{r}
ggplot(as.data.frame(pca$pc),aes(PC1,PC2)) +
  geom_text(aes(label=row.names(pca$pc)),size=3,alpha=1,vjust =-1,hjust=0.5) +
  geom_segment(aes(x=0,xend=PC1,y=0,yend=PC2),arrow=arrow(length=unit(0.3,"cm")),col="darkblue") +
  labs(title="PCA",
       x=paste("PC1:",round(pca$explained_variance[1]*100,2),"% projected variance"),
       y=paste("PC2:",round(pca$explained_variance[2]*100,2),"% projected variance"))
```

Plot *PCA* projections coloured by predictions of the models
```{r}
# Plot PCA projections coloured by predictions of the model Logistic Regression
Prediction_LR <- c(pred_LR_train,pred_LR_test)
ggplot(pca_projections,aes(PC1,PC2,color=Prediction_LR)) +
  geom_point() + scale_colour_gradient(low="green",high="red") +
  xlim(-5,5) + ylim(-5,4) + 
  labs(title="PCA Projections: Model Logistic Regression",
       x=paste("PC1:",round(pca$explained_variance[1]*100,2),"% variance"),
       y=paste("PC2:",round(pca$explained_variance[2]*100,2),"% variance"))

# Plot PCA projections coloured by predictions of the model Decision Tree
Prediction_DT <- c(pred_DT_train,pred_DT_test)
ggplot(pca_projections,aes(PC1,PC2,color=Prediction_DT)) +
  geom_point() + scale_colour_gradient(low="green",high="red") +
  xlim(-5,5) + ylim(-5,4) + 
  labs(title="PCA Projections: Model Decision Tree",
       x=paste("PC1:",round(pca$explained_variance[1]*100,2),"% variance"),
       y=paste("PC2:",round(pca$explained_variance[2]*100,2),"% variance"))

# Plot PCA projections coloured by predictions of the model Random Forest
Prediction_RF <- c(pred_RF_train,pred_RF_test)
ggplot(pca_projections,aes(PC1,PC2,color=Prediction_RF)) +
  geom_point() + scale_colour_gradient(low="green",high="red") +
  xlim(-5,5) + ylim(-5,4) + 
  labs(title="PCA Projections: Model Random Forest",
       x=paste("PC1:",round(pca$explained_variance[1]*100,2),"% variance"),
       y=paste("PC2:",round(pca$explained_variance[2]*100,2),"% variance"))

# Plot PCA projections coloured by predictions of the model Gradient Boosted Trees
Prediction_GBT <- c(pred_GBT_train,pred_GBT_test)
ggplot(pca_projections,aes(PC1,PC2,color=Prediction_GBT)) +
  geom_point() + scale_colour_gradient(low="green",high="red") +
  xlim(-5,5) + ylim(-5,4) + 
  labs(title="PCA Projections: Model Gradient Boosted Trees",
       x=paste("PC1:",round(pca$explained_variance[1]*100,2),"% variance"),
       y=paste("PC2:",round(pca$explained_variance[2]*100,2),"% variance"))

# Plot PCA projections coloured by predictions of the model Naive Bayes
Prediction_NB <- c(pred_NB_train,pred_NB_test)
ggplot(pca_projections,aes(PC1,PC2,color=Prediction_NB)) +
  geom_point() + scale_colour_gradient(low="green",high="red") +
  xlim(-5,5) + ylim(-5,4) + 
  labs(title="PCA Projections: Model Naive Bayes",
       x=paste("PC1:",round(pca$explained_variance[1]*100,2),"% variance"),
       y=paste("PC2:",round(pca$explained_variance[2]*100,2),"% variance"))

# Plot PCA projections coloured by predictions of the model Multilayer Perceptron
Prediction_NN <- c(pred_NN_train,pred_NN_test)
ggplot(pca_projections,aes(PC1,PC2,color=Prediction_NN)) +
  geom_point() + scale_colour_gradient(low="green",high="red") +
  xlim(-5,5) + ylim(-5,4) + 
  labs(title="PCA Projections: Model Multilayer Perceptron",
       x=paste("PC1:",round(pca$explained_variance[1]*100,2),"% variance"),
       y=paste("PC2:",round(pca$explained_variance[2]*100,2),"% variance"))
```

# 9. DISCONNECTION OF SPARK CLUSTERS

Disconnect the *Spark* session of both *SparkR* and *sparklyr* packages
```{r}
spark_disconnect(sc_sparklyr)
sparkR.session.stop()
```
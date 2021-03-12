#UtilityFunctions ####
#' @title Utility Functions
#' @name UtilityFunctions
NULL

##########################
# PAWR maintenance tools #
##########################

#' @describeIn UtilityFunctions re-fetches the rate limit and parameter list from pushshift.io
#' @param verbose Logical. Produce verbose output or not.
#' @export
refreshPAWR<-function(verbose=T){
  #Settings setup
  if(is.null(getOption("PAWR.VerboseGet"))){
    options(PAWR.VerboseGet=F)
  }
  if(is.null(getOption("PAWR.VerbosePaginate"))){
    options(PAWR.VerbosePaginate=F)
  }
  if(is.null(getOption("PAWR.VerboseRateLimit"))){
    options(PAWR.VerboseRateLimit=F)
  }
  if(is.null(getOption("PAWR.UserAgent"))){
    options(PAWR.UserAgent="Pushshift API Wrapper for R (PAWR)")
  }
  if(is.null(getOption("PAWR.QuerySize"))){
    options(PAWR.QuerySize=100)
  }

  #Rate limit
  reqinfo<-NULL
  reqinfo<-RETRY("GET",url="http://api.pushshift.io/meta",timeout(10),
                 user_agent(getOption("PAWR.UserAgent")))%>%content()
  if(is.null(reqinfo)){
    stop("Pushshift.io is down, or you are not connected to the internet.")
  }
  .rlims<<-rep(0,reqinfo$server_ratelimit_per_minute)
  .msgWhen(when=verbose, "Rate limit is: ",reqinfo$server_ratelimit_per_minute," requests per minute.")

  #Get endpoints
  pts<-RETRY("GET","https://pushshift.io/api-parameters/",timeout(10),
             user_agent(getOption("PAWR.UserAgent")))%>%content()
  .psparams<<-data.frame(parameter  =pts%>%rvest::html_nodes(".column-1")%>%rvest::html_text()%>%trimws,
                         type       =pts%>%rvest::html_nodes(".column-2")%>%rvest::html_text()%>%trimws,
                         endpoint   =pts%>%rvest::html_nodes(".column-3")%>%rvest::html_text()%>%trimws,
                         description=pts%>%rvest::html_nodes(".column-4")%>%rvest::html_text()%>%trimws,
                         stringsAsFactors = F)[-1,]
  .psparams$endpoint<<-gsub(" Endpoints?","", .psparams$endpoint) %>% tolower
}

.checkRateLimit<-function(){
  return((as.numeric(Sys.time())-.rlims[length(.rlims)]))
}

.bumpRateLimit<-function(){
  .rlims<<-c(as.numeric(Sys.time()),.rlims[1:(length(.rlims)-1)])
}

.awaitRateLimit<-function(verbose=getOption("PAWR.VerboseRateLimit")){
  rlim<-.checkRateLimit()
  if(rlim<60){
    .msgWhen("Rate limit exceeded! Sleeping until requests can be made again.",when=verbose)
    Sys.sleep(60.1-rlim)
  }
  .bumpRateLimit()
}

#' @describeIn UtilityFunctions returns all available pushshift.io parameters for a given data type; defaults to all data types.
#' @param type Character. The type of content that parameters are being looked up for (comment, submission, subreddit). Defaults to all.
#' @export
PSParams<-function(type=c("all","comment","submission","subreddit")){
  type<-match.arg(type)
  print(type)
  if(type=="all"){
    out<-.psparams
  }else{
    out<-.psparams[.psparams$endpoint%in%c("all",type),]
  }
  return(out)
}

#' @describeIn UtilityFunctions gives the total amount of content that matches the provided parameters
#' @param ... For GetTotalQuerySize, any parameters to be sent to pushshift.io
#' @export
GetTotalQuerySize<-function(...){
  args<-list(...)
  args$size<-1
  args$metadata<-T
  args$verbose<-F
  args$as.df=F
  out<-do.call(QueryPushshift,args)
  return(out$metadata$total_results)
}

.checkfields<-function(fields,type){
  if(exists(".psparams")){
    okfields <-.psparams[.psparams$endpoint%in%c("all",type),]$parameter
    #add to-be-ignored words
    okfields<-c(okfields,"before","after","timescope","fields","aggs")
    except <- which(!(fields %in% okfields))
    if(length(except)>0){
      stop("The following parameter(s) are unknown to pushshift.io: ",
           paste(fields[except],collapse=" "),
           "\nUse PSParams() to see all available parameters.")
    }else{ return(TRUE)  }
  }else{
    return(TRUE)
  }
}



######################
# data merging tools #
######################

#' @describeIn UtilityFunctions converts a list of lists (pushshift's preferred output format) to a data.frame
#' @param li List of lists, to be converted to \code{data.frame}.
#' @export
list2df<-function(li){
  if(length(li)==0){
    return(NULL)
  }else{
    out<-lapply(li,function(x){
      flatx<-flatten(x)
      flatx<-flatx[sapply(flatx,length)>0]
      if(length(flatx)>0){
        fixColNames(as.data.frame(flatx))
      }else{
        NULL
      }
    })
    out<-do.call(unevenrbind,out)
    return(out)
  }
}

#' @describeIn UtilityFunctions binds two data.frames together by row, even if their columns do not match
#' @param ... To-be-merged data frames with an uneven number of columns and/or nonmatching column names
#' @export
unevenrbind<-function(...){
  args<-list(...)
  nameset<-unique(unlist(sapply(args,colnames)))
  addcols<-
    function(x){
      missingnames<-setdiff(nameset, names(x))
      empties<-setNames(as.data.frame(matrix(NA,ncol=length(missingnames),nrow=nrow(x))),missingnames)
      cbind(x,empties)
    }
  dflist <- lapply(args, addcols)
  out<-do.call(rbind,dflist)
  return(out)
}

fixColNames<-function(df){
  nv<-colnames(df)
  dupes<-unique(nv[duplicated(nv)])
  for(p in dupes){
    nv[nv==p]<-paste0(p,"_",seq_len(sum(nv==p)))
  }
  colnames(df)<-nv
  return(df)
}

flatten<-function(x){
  while("list" %in% lapply(x,class)){
    x<-unlist(x,F)
  }
  return(x)
}


#########
# other #
#########

#' @describeIn UtilityFunctions Get the timestamp of the current moment.
#' @export
now<-function(){
  return(round(as.numeric(Sys.time())))
}

.msgWhen <-function(...,when=T){
  if(when){message(...)}
}






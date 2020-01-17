#' PAWR: The Pushshift API Wrapper for R.
#'
#' @docType package
#' @name PAWR
#' @section Package options:
#' \itemize{
#'  \item{\code{PAWR.VerboseGet} designates whether the main data retrieval function should produce verbose output. Useful when debugging.}
#'  \item{\code{PAWR.VerbosePaginate} designates whether pagination functions should produce verbose output.}
#'  \item{\code{PAWR.UserAgent} is the useragent used by PAWR when querying pushshift.io.}
#' }
NULL

#require(httr)
#require(rvest)
#require(magrittr)
#library(dplyr)

.onAttach<-function(libname,pkgname){
  .rlims<<-NULL
  try(refreshPAWR(verbose=F))
  packageStartupMessage("Thank you for loading the Pushshift API Wrapper for R (PAWR).")
  #add pushshift status message here
}

#UtilityFunctions ####
#' @title Utility Functions
#' @name UtilityFunctions
#' @details
#' \code{refreshPAWR()} re-fetches the rate limit and parameter list from pushshift.io
#' \code{list2df()} converts a list of lists (pushshift's preferred output format) to a data.frame
#' \code{unevenrbind()} binds two data.frames together by row, even if their columns do not match
#' \code{PSParams()} returns all available pushshift.io parameters for a given data type; defaults to all data types.
#' \code{GetTotalQuerySize()} gives the total amount of content that matches the provided parameters
NULL

#' @param verbose Logical. Produce verbose output or not.
#' @export
#' @name UtilityFunctions
refreshPAWR<-function(verbose=T){
  #Settings setup
  if(is.null(getOption("PAWR.VerboseGet"))){
    options(PAWR.VerboseGet=F)
  }
  if(is.null(getOption("PAWR.VerbosePaginate"))){
    options(PAWR.VerbosePaginate=F)
  }
  if(is.null(getOption("PAWR.UserAgent"))){
    options(PAWR.UserAgent="Pushshift API Wrapper for R (PAWR)")
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

#' @param li List of lists, to be converted to \code{data.frame}.
#' @export
#' @name UtilityFunctions
list2df<-function(li){
  lnames<-lapply(li,FUN=function(x){names(unlist((x)))})%>%unlist%>%unique
  newdf<-setNames(data.frame(matrix(ncol = length(lnames), nrow = 0),stringsAsFactors=F), lnames)
  for(i in 1:length(li)){
    rowdat<-li[[i]]%>%unlist%>%t%>%as.data.frame(stringsAsFactors=F)
    rowdat[,setdiff(names(newdf), names(rowdat))] <- NA
    newdf[,setdiff(names(rowdat), names(newdf))] <- NA
    newdf<-rbind(newdf,rowdat)
  }
  newdf
}

#' @param df1,df2 To-be-merged data frames with an uneven number of columns and/or nonmatching column names
#' @export
#' @name UtilityFunctions
unevenrbind<-function(df1,df2){
  rbind(
    data.frame(c(df1, sapply(setdiff(names(df2), names(df1)), function(x) NA)),stringsAsFactors=F),
    data.frame(c(df2, sapply(setdiff(names(df1), names(df2)), function(x) NA)),stringsAsFactors=F),
    stringsAsFactors=F
  )
}

#' @param type Character. The type of content that parameters are being looked up for (comment, submission, subreddit). Defaults to all.
#' @export
#' @name UtilityFunctions
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

#' @param ... For GetTotalQuerySize, any parameters to be sent to pushshift.io
#' @export
#' @name UtilityFunctions
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
  okfields <-.psparams[.psparams$endpoint%in%c("all",type),]$parameter
  #add to-be-ignored words
  okfields<-c(okfields,"before","after","timescope","fields","aggs")
  except <- which(!(fields %in% okfields))
  if(length(except)>0){
    stop("The following parameter(s) are unknown to pushshift.io: ",paste(fields[except],collapse=" "),
         "\nUse PSParams() to see all available parameters.")
  }else{ return(TRUE)  }
}

.msgWhen <-function(...,when=T){
  if(when){message(...)}
}

#Main function ####
#' @title Query data from pushshift.io
#'
#' @param type Type of requested content. Can be \code{comment}, \code{submission}, or \code{subreddit}.
#' @param as.df Convert output to \code{data.frame}? Defaults to \code{TRUE}.
#' @param purge Purge deleted posts? Defaults to \code{FALSE}.
#' @param verbose Should output be verbose? Defaults to a global option which can be set with \code{options(PAWR.VerboseGet=TRUE/FALSE)}.
#' @param size Maximum number of pieces of content to return. Defaults to the maximum, which is 500, except when using aggs, when it defaults to 0.
#' @param agg_size Maximum number of values to return when using aggs; defaults to 500 unless you're not using aggs.
#' @param q Query term.
#' @param metadata Request metadata from pushshift, which will be used to check whether the query was successful or some of pushshift's shards failed to respond. This is recommended for academic research.
#' @param ... Other valid parameters. Run \code{PSParams()} to see all valid parameters and their descriptions.
#'
#' @return If \code{as.df=T}, returns a data.frame; else, returns a list.
#' @export
#'
#' @examples
#' #Get u/spez's first ever comment
#' QueryPushshift(author="spez",after=0,size=1)
#'
#' #See in which subreddits the word "gamer" is used the most
#' GetPSData(q="Gamer",aggs="subreddit")
QueryPushshift<-function(type=c("comment","submission","subreddit"),as.df=T,purge=F,verbose=getOption("PAWR.VerboseGet"),
                    size=500,aggs=c("none","author","link_id","created_utc","subreddit"),agg_size=0,q=NULL,metadata=TRUE,
                    ...){

  type<-match.arg(type)
  aggs<-match.arg(aggs)
  args<-list(...)
  .checkfields(names(args),type)

  #form the GET url
  anames<-names(args)
  argstring<-""
  if(length(args)>0){
    for(i in 1:length(args)){
      argstring%<>%paste0("&",anames[i],"=",paste(args[[i]],collapse=","))
    }
  }

  #deal with aggs
  if(aggs!="none"){
    if(missing(size)){
      size<-0
    }
    if(missing(agg_size)){
      .msgWhen("Aggs size unspecified, defaulting to 500",when=verbose)
      agg_size<-500
    }
    argstring%<>%paste0("&aggs=",aggs,"&agg_size=",agg_size)
  }

  if(metadata){ argstring%<>%paste0("&metadata=true")}
  if(!is.null(q)){ argstring%<>%paste0("&q=",URLencode(paste(q,collapse="|")))}

  reqstr<-paste0("https://api.pushshift.io/reddit/search/",type,"?size=",size,argstring)

  #Check the rate limit.
  rlim<-.checkRateLimit()
  if(rlim<60){
    .msgWhen("Rate limit exceeded! Sleeping until requests can be made again.",when=verbose)
    Sys.sleep(60.1-rlim)
  }
  .bumpRateLimit()

  #try getting the data
  .msgWhen(when=verbose,"Getting data from URL ",reqstr)
  reqinfo<-RETRY("GET",url=URLencode(reqstr),timeout(10),user_agent(getOption("PAWR.UserAgent"))) %>% content(encoding="UTF-8")

  #purge deleted posts
  if(purge){
    deldposts<-sapply(reqinfo$data,FUN=function(x){x$author})!="[deleted]"
    reqinfo$data<-reqinfo$data[deldposts]
  }


  #convert list to data.frame
  if(as.df){
    if(length(reqinfo$data)>0){
      datadf<-list2df(reqinfo$data)

      #warn if cols absent
      if("fields" %in% tolower(anames)){
        if(sum(!(args$fields %in% colnames(datadf)))>0){
          warning("Not all requested fields are present: ", paste(args$fields[!(args$fields %in% colnames(datadf))],collapse=", "))
        }
      }
    }
    if(length(reqinfo$aggs%>%unlist(F))>0){
      aggdf<-list2df(reqinfo$aggs%>%unlist(F))
    }

    if(exists("datadf") & exists("aggdf")){ output<-list(data=datadf,aggs=aggdf) }
    else if(exists("datadf")){ output<-datadf }
    else if(exists("aggdf")){ output<-aggdf }
    else return(NULL)

  }else{
    output<-reqinfo
  }

  #check metadata
  if(metadata){
    if(any(reqinfo$metadata$shards$failed>0)){
      warning("One more shards have failed! This means the data returned may be incomplete.
              It may be better to collect your data another time.")
    }
  }
  return(output)
}

# paginate functions ####

#' @title Paginate PushShift Data
#' @description Send multiple queries to pushshift.io, to be able to get all data within a given date range.
#'
#' @param type Type of requested content. Can be \code{comment}, \code{submission}, or \code{subreddit}.
#' @param verbose Should output be verbose? Defaults to a global option which can be set with \code{options(PAWR.VerbosePaginate=TRUE/FALSE)}.
#' @param before Upper limit in the date range of posts to be fetched.
#' @param after Lower limit in the date range of posts to be fetched.
#' @param timescope Time range, in seconds, within which posts should be fetched;
#' works in conjunction with either \code{before} or \code{after}.
#' When argument \code{after} is used, \code{timescope} causes the current function to fetch data that was created up to \emph{N} seconds after the timestamp defined in \code{after}
#' When argument \code{before} is used, \code{timescope} causes the current function to fetch data that was created up to \emph{N} seconds before the timestamp defined in \code{before}
#' @param ... Other valid parameters. Run \code{PSParams()} to see all valid parameters and their descriptions.
#'
#' @return
#' @export
#'
#' @examples
#' #Get all comments from today containing the word "chocolate"
#' PaginateData(timescope=24 * 60 * 60,q="chocolate")
PaginateData<-function(type=c("comment","submission","subreddit"),verbose=getOption("PAWR.VerbosePaginate"),
                        before=round(as.numeric(Sys.time())),after=NULL,timescope=NULL,
                        ...){
  args<-list(...)
  .checkfields(names(args),type)

  #check args
  if(missing(after) & missing(timescope)){
    stop("Please provide a range of time from which to scrape, using args after= or timescope=")
  }else if(missing(after)){
    after<-before-timescope
  }else if(missing(before)){
    before<-after+timescope
  }

  if(any(is.null(args$fields))){
    warning("You did not specify any fields! This can lead to highly redundant output.")
  }else
  if(any(!("created_utc" %in% args$fields))){
    args$fields%<>%c("created_utc")
  }

  lastdate<-after
  output<-NULL
  while(lastdate+1<before){
    loopoutput<-do.call(QueryPushshift,c(args,list(purge=F,type=type,after=lastdate,before=before)))
    if(is.null(output)){
      output<-loopoutput
    }else{
      output<-bind_rows(output,loopoutput)
    }
    lastdate<-max(as.numeric(output$created_utc))-1
    #message("lastdate: ",lastdate,", after: ",after)
    if(nrow(loopoutput)<500){ break; }
  }
  #loopoutput
  output[(output$created_utc > after) & (output$created_utc < before) & !duplicated(output),]
}


# paginate aggs ####

#' @title Paginate aggs
#' @description Send multiple queries to pushshift.io to get all available information for your request.
#' This function is meant to get around the maximum of 1000 items returned by a single aggs query.
#'
#' @param type Type of requested content. Can be \code{comment}, \code{submission}, or \code{subreddit}.
#' @param aggs What should be aggregated over?
#' @param paginate_by Define which variable should be used to break the data into smaller chunks; either \code{author} or \code{date}.
#' @param verbose Should output be verbose? Defaults to a global option which can be set with \code{options(PAWR.VerbosePaginate=TRUE/FALSE)}.
#' @param ... Other valid parameters. Run \code{PSParams()} to see all valid parameters and their descriptions.
#'
#' @return
#' @export
#'
#' @examples
#' #Find out on which subreddits the users of r/cheese post
#' #Analysis is limited to December 2019
#' users<-PaginateAggs(aggs="author",paginate_by="date",
#'   subreddit="cheese",timescope=30*24*60*60,before=1577836800)
#' users<-users$key
#' #remove bots and missing values
#' users<-users[!(users %in% c("[deleted]","AutoModerator"))]
#' #Posting behavior of all authors is aggregated.
#' subreddits<-PaginateAggs(aggs="subreddit",paginate_by="author",
#'   author=users,timescope=30*24*60*60,before=1577836800)
PaginateAggs<-function(type=c("comment","submission","subreddit"),aggs=c("author","link_id","created_utc","subreddit"),
                       paginate_by=c("date","author"), before=round(as.numeric(Sys.time())), after=NULL,timescope=NULL,
                       verbose=getOption("PAWR.VerbosePaginate"), ...){
  type<-match.arg(type)
  aggs<-match.arg(aggs)
  paginate_by<-match.arg(paginate_by)
  args<-list(...)
  .checkfields(names(args),type)

  args<-c(args,type=type,aggs=aggs,verbose=verbose,before=before,after=after,timescope=timescope)
  if(tolower(paginate_by)=="date"){
    do.call(PaginateAggsByDate,args)
  }else if(tolower(paginate_by)=="author"){
    do.call(PaginateAggsByAuthor,args)
  }
}

PaginateAggsByDate<-function(type=c("comment","submission","subreddit"),verbose=getOption("PAWR.VerbosePaginate"),
                             before=round(as.numeric(Sys.time())),after=NULL,timescope=NULL,
                             stepsize=120,upstep=2,downstep=10,
                             ...){
  type<-match.arg(type)
  args<-list(...)

  upstreak=0
  downstreak=0
  maxcount=999
  mincount=900

  if(is.null(timescope)){
    if(is.null(after)){ stop("Specify either 'after' or 'timescope' please.")}
    timescope<-before-after
  }else if(is.null(after)){
    after<-before-timescope
  }else if(missing(before)){
    before<-after+timescope
  }

  if(missing(stepsize)){
    stepsize<-timescope
    downstep<-ceiling(stepsize/10)
  }

  currtime<-before
  output<-data.frame(key=NA,doc_count=NA,stringsAsFactors=F)[F,]
  while(before-currtime<timescope){

    iterdat<-do.call(QueryPushshift,c(args,list(before=currtime,after=max(after,currtime-stepsize),
                                           type=type,agg_size=1000)))
    #message("Scope: ",currtime-stepsize," - ",currtime)
    if(!is.null(iterdat)){
      if(nrow(iterdat) > maxcount){
        #message("Too much data returned! Narrowing scope! ", stepsize)
        downstreak<-downstreak+1
        stepsize<-max(1,ceiling(stepsize-stepsize*0.1*downstreak))

      }else{
        if(nrow(iterdat) < mincount){
          .msgWhen(when=verbose,"Too little data returned! Expanding scope! ", stepsize)
          upstreak<-upstreak+1
          stepsize<-stepsize+upstep*upstreak
        }else{
          upstreak<-0
          downstreak<-0
        }
        currtime<-currtime-stepsize
        output<-rbind(output,iterdat)
      }
      }else{
        .msgWhen(when=verbose,"Too little data returned! Expanding scope! ", stepsize)
        upstreak<-upstreak+1
        stepsize<-stepsize+upstep*upstreak
        currtime<-currtime-stepsize
      }


    cat(sep="","\rCUrrent progress: ",round(100*(before-currtime)/timescope,digits=2),"%")
  }

  output%<>%group_by(key)%>%summarise(doc_count=sum(as.numeric(doc_count)))%>%group_by()%>%arrange(desc(doc_count))
  return(output)
}




PaginateAggsByAuthor<-function(type=c("comment","submission","subreddit"),verbose=getOption("PAWR.VerbosePaginate"),
                               author=NULL,aggs=NULL,
                             before=round(as.numeric(Sys.time())),after=NULL,timescope=NULL,
                             stepsize=40,upstep=2,downstep=2,
                             ...){
  type<-match.arg(type)
  args<-list(...)
  .checkfields(names(args),type)
  stopifnot(!is.null(author))
  stopifnot(!is.null(aggs))

  if(!is.null(timescope)){
    if(is.null(after)){
      after<-before-timescope
    }else if(missing(before)){
      before<-after+timescope
    }
  }

  maxcount=999
  mincount=900
  upstreak<-0
  downstreak<-0
  index<-1
  output<-data.frame(key=NA,doc_count=NA,stringsAsFactors=F)[F,]
  while(index<=length(author)){
    range<-index:(min(index+stepsize-1,length(author)))
    currauth<-author[range]
    iterdat<-do.call(QueryPushshift,c(args,list(before=before,after=after,type=type,agg_size=1000,
                                           author=currauth,aggs=aggs)))
    if(!is.null(iterdat)){
      if(nrow(iterdat) > maxcount){
        .msgWhen(when=verbose,"Too much data returned! Narrowing scope! ", stepsize)
        downstreak<-downstreak+1
        stepsize<-max(1,stepsize-downstep*downstreak)

      }else{
        if(nrow(iterdat) < mincount){
          .msgWhen(when=verbose,"Too little data returned! Expanding scope! ", stepsize)
          upstreak<-upstreak+1
          stepsize<-stepsize+upstep*upstreak
        }else{
          upstreak<-0
          downstreak<-0
        }
        index<-index+stepsize
        output<-rbind(output,iterdat)
      }
    }else{
      .msgWhen(when=verbose,"Too little data returned! Expanding scope! ", stepsize)
      upstreak<-upstreak+1
      stepsize<-stepsize+upstep*upstreak
      index<-index+stepsize
    }


    cat(sep="","\rCUrrent progress: ",min(round(index/length(author)*100,digits=2),100),"%")
  }

  output%<>%group_by(key)%>%summarise(doc_count=sum(as.numeric(doc_count)))%>%group_by()%>%arrange(desc(doc_count))
  return(output)
}

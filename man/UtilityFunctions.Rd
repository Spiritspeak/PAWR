% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{UtilityFunctions}
\alias{UtilityFunctions}
\alias{refreshPAWR}
\alias{PSParams}
\alias{GetTotalQuerySize}
\alias{list2df}
\alias{unevenrbind}
\alias{now}
\title{Utility Functions}
\usage{
refreshPAWR(verbose = T)

PSParams(type = c("all", "comment", "submission", "subreddit"))

GetTotalQuerySize(...)

list2df(li)

unevenrbind(...)

now()
}
\arguments{
\item{verbose}{Logical. Produce verbose output or not.}

\item{type}{Character. The type of content that parameters are being looked up for (comment, submission, subreddit). Defaults to all.}

\item{...}{To-be-merged data frames with an uneven number of columns and/or nonmatching column names}

\item{li}{List of lists, to be converted to \code{data.frame}.}
}
\description{
Utility Functions
}
\section{Functions}{
\itemize{
\item \code{refreshPAWR}: re-fetches the rate limit and parameter list from pushshift.io

\item \code{PSParams}: returns all available pushshift.io parameters for a given data type; defaults to all data types.

\item \code{GetTotalQuerySize}: gives the total amount of content that matches the provided parameters

\item \code{list2df}: converts a list of lists (pushshift's preferred output format) to a data.frame

\item \code{unevenrbind}: binds two data.frames together by row, even if their columns do not match

\item \code{now}: Get the timestamp of the current moment.
}}


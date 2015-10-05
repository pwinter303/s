#!/usr/bin/perl
use strict;
use Data::Dumper;
use Text::ParseWords;
use HTML::TableExtract;
use threads;
use Switch;

#### 2015-09-12... Written. Purpose: Takes output from stockDataDownload.pl and processes it.. Creating a single output file for each type of file
####                            for example... the 5,000 YahooIndustry input files are written to a single file with all 5,000 tickers

## The higher the number the more is output... WIP...
my $DEBUG = 0;
my $OUTPUTTEMPFILE = 0;

require "myFunctions.pl";

### Standard Block of Code used in stock processing scripts......
my $configHashRef = getConfig('stockData.config');
my $pathToSymbolFile = $configHashRef->{'stocks.symbolPath'};
my $pathToDataFiles = $configHashRef->{'stocks.dataPath'};
my $symbolFileName = $configHashRef->{'stocks.symbolFileName'};

my $total = $#ARGV + 1;
if ($total) {
    $symbolFileName = shift @ARGV;
    print "symbol file name was overidden via passed argument. This symbol file will be used:$symbolFileName\n ";
}

#getTickers is part of myFunctions.. gets all tickers from the file its passed
my $fullFilePathAndName = $pathToSymbolFile . $symbolFileName;
my $tickerArrRef = getTickers($fullFilePathAndName);
my $arrayOfArrayRef = splitArray(25,$tickerArrRef);

## Where to Write OutputFiles
my $pathOutput = $configHashRef->{'stocks.consolidateFilePath'};

### To setup a new file
###     run table dumper to identify tables required
###     Setup table: depth and counts required
###     set up regEx
###     setup standardConfig: Name of input, output, etc
#
#NOTE: When getting HTML to match....
#       read html file, go through the clean subroutine, output results to a temp file
#       open temp file in brackets or text edit (NOT a browser)
#       then go through and get the HTML to match.. if you copy it within browser or before cleans
#       the results will be different

#TEST AREA
# my $dataName = "ZacksIndustry";
# mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);
# die;

### Market Watch: Yearly: Balance Sheet
my $dataName = "MarketWatchYrlyBalSheet";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### Market Watch: Quarterly: Balance Sheet
my $dataName = "MarketWatchQtrlyBalSheet";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### Market Watch: Yearly: Income Statement
my $dataName = "MarketWatchYrlyIncStmt";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### Market Watch: Quarterly: Income Statement
my $dataName = "MarketWatchQtrlyIncStmt";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### Market Watch: Yearly: CashFlow
my $dataName = "MarketWatchYrlyCashFlow";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### Market Watch: Quarterly: CashFlow
my $dataName = "MarketWatchQtrlyCashFlow";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### YAHOO INDUSTRY
my $dataName = "YahooIndustry";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### ZACKS INDUSTRY
my $dataName = "ZacksIndustry";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

### ZACKS ESTIMATES
my $dataName = "ZacksEstimates";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

#### YAHOO ANALYST
my $dataName = "YahooAnalyst";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

#### YAHOO KEY STATS
my $dataName = "YahooKeyStats";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

#### YAHOO Estimates
my $dataName = "YahooEstimates";
mainProcess($pathToDataFiles, $pathOutput, $tickerArrRef, $dataName);

END {
  warn "The whole script took ", time - $^T, " seconds\n";
}


############# SUBROUTINES ##########################

sub mainProcess{
    my $pathToDataFiles = shift @_;
    my $pathOutput = shift @_;
    my $tickerArrRef = shift @_;
    my $dataName = shift @_;

    my $hashConfigRef = getStandardConfigs($dataName);
    my $pathInput = $pathToDataFiles . $hashConfigRef->{"inputPathSuffix"};
    ##print "pathInput:$pathInput\n";
    my  $inFileSuffix = $hashConfigRef->{"inFileSuffix"};

    my $now = time;
    my @threads;
    foreach my $tickerArrRef (@$arrayOfArrayRef){
        my %dataHash;
        push @threads, threads->create(\&multiThreadProcess,$pathInput, \%dataHash,
                                        $tickerArrRef, $inFileSuffix, $dataName);
    }

    ### wait for all the threads to finish
    ### then create an array of the hashReferences returned in the multiThreadedMainProcessing subroutine
    my @results;
    foreach (@threads) {
        push (@results, $_->join());
    }
    ## merge the array of HashReferences that were returned from threads into a single Hash
    my %results;
    foreach my $reg (@results){
        %results = (%results, %$reg);
    }

    if ($DEBUG > 0){
      print "RESULTS ARE IN... \n";
      ## hash is keyed by Ticker. Then DataName. Then Field
      for my $ticker ( sort keys %results ) {
                  print "Ticker:$ticker \n   dataName:$dataName:\n";
                  for my $fieldKey (sort keys $results{$ticker}{$dataName}){
                    print "\t\t$fieldKey->$results{$ticker}{$dataName}{$fieldKey}\n";
                  }
      }
    }
    ### after the thread ends all the refences are empty... FixMe: is there a better way to do this?
    my $outFile = $pathOutput . $hashConfigRef->{"outputFileName"};
    open (my $outFH, ">", $outFile) or die "Cannot open $outFile!\n";

    my $keysArrRef = getKeysWeWant($dataName);

    outputResults($outFH, $dataName, $keysArrRef, \%results);
    close $outFH;

    runTimeNow($now);

}



sub multiThreadProcess{
    my $pathInput = shift @_;
    my $dataHashRef = shift @_;
    my $tickerArrRef = shift @_;
    my $inFileSuffix = shift @_;
    my $dataName = shift @_;

    my $thr = threads->self();
    my $tid = $thr->tid();

    print "$dataName\tStarted Thread:$tid\n";
    my $symbolsProcessed = 0;
    my $itemsInArray = @$tickerArrRef;
    foreach my $ticker (@$tickerArrRef){
        $symbolsProcessed++;
        unless ($symbolsProcessed % 25){
          #print "Thread:$tid\t$dataName\tTicker:$ticker\tThis is ticker number:$symbolsProcessed\n";
          print "Thread:$tid\t$dataName\tProcessed $symbolsProcessed out of $itemsInArray Tickers\n";

        }
        my $inFile = $pathInput . $ticker . $inFileSuffix;
        processTicker($ticker, $inFile, $dataHashRef, $dataName);
    }
    return $dataHashRef;
}




sub processTicker{
    my $ticker = shift @_;
    my $inFile = shift @_;
    my $dataHashRef = shift @_;
    my $dataName = shift @_;

    $dataHashRef->{$ticker}{$dataName}{'LastModDt'} = getLastModifiedDate($inFile);
    my $content = getData($inFile);
    $content = cleanContent($content);


    ###uncomment to generate a temp file used for HTML matching in extractSpecial
    ### NOTE: may want to wrap if around this and check $dataName and only output for certain file types
    if ($OUTPUTTEMPFILE){
          my $fileName = "/Users/pwinter303/Documents/Scripts/ContentAfterClean2.html";
          # # my $fileName = "C:/Users/paul-winter/Desktop/Junk-DeleteMe/Stocks/JunkZackEstPostClean.html";
          print "\n\nOUTPUTTING TEMP FILE:$fileName\n\n";
          open my $tmpFG, ">", $fileName;
          print $tmpFG $content;
          close $tmpFG;
    }

    extractMain($ticker, $content, $dataName, $dataHashRef);

    ###FixMe:  See notes in: getZacksEstimateKeys   regarding a post process.. After all the data has been extracted do a final cleanse.. tweak
    #print Dumper($dataHashRef);
}


#######################
sub getData{
    my $inFile = shift @_;

    my $content = "";
    my $handle;
    open ($handle, "<", $inFile) || print "\n\n!!!!!!!!!!!!!!!!!!! ERROR ERROR!!!!!!!!!!!!!:\n Cant open $inFile\n\n";
    local $/ = undef;
    chomp($content = <$handle>);
    close $handle;
    return $content;
}

#######################
sub cleanContent{
    my $content = shift @_;

  ##$content =~ s/\n//gs;    ### Strip Carriage Returns
  $content =~ s/\n|\r//gs;    ### Strip Carriage Returns
	$content =~ s/&nbsp;//gs;


  #### COMMENTING OUT SO NEGATIVES WORK!
  #FixMe... what about negative numbers in the financials!!
  ###$content =~ s/\(//gs;    ### Strip parentheses  - PARENTHESES MUST BE STRIPPED OR MATCH DOES NOT WORK...
	###$content =~ s/\)//gs;    ### Strip parentheses
	$content =~ s/&amp;/&/g; ### Fix ampersands

    #from ZackEstimate.pl
    $content =~ s/[^[:ascii:]]+//g; ## this strips out any non-ascii characters

    return $content;
}


#######################
sub extractMain{
    my $ticker = shift @_;
    my $content = shift @_;
    my $dataName = shift @_;
    my $dataHashRef = shift @_;

    #FixMe: get tableDepth
    my $tableDepthCountsNeededRef = getTableDepthsNeeded($dataName);
    ##print "\ntableDepthCountsNeededRef:" . Dumper($tableDepthCountsNeededRef) . "<<<--\n";
    my $fieldsWeWantFromTablesHashRef = getFieldsWeWantFromTables($dataName);
    ##print "\nfieldsWeWantFromTablesHashRef:" . Dumper($fieldsWeWantFromTablesHashRef) . "<<<--\n";

    my %tableData;
    foreach my $depthKey (keys %$tableDepthCountsNeededRef){

        my @arrCounts = @{$tableDepthCountsNeededRef->{$depthKey}};
        ## This function extracts all the tables for a specific depth that match the counts held in arrCounts
        extractTables($content, $depthKey, \@arrCounts, \%tableData);
        if ($DEBUG > 4){
            print "-------->>>>>>    FINISHED PULLING TABLES FROM THE WEB PAGE.  TABLES:" . Dumper(%tableData) . "<<<<<---\n";
        }

        ### This will extracts data with matching keys
        extractDataKeyValuePairNEW($ticker, \%tableData, $dataHashRef, $dataName, $fieldsWeWantFromTablesHashRef);
        if ($DEBUG > 4){
            print "-------->>>>>>    FINISHED EXTRACTING DATA FROM THE TABLES.  DataHash is:" . Dumper($dataHashRef);
        }
    }

    my $fieldsWeWantViaRegExHashRef = getFieldsWeWantViaRegEx($dataName);
    extractRegEx($ticker,$dataName, $dataHashRef, $content, $fieldsWeWantViaRegExHashRef);
    #print "-------->>>>>>    FINISHED REGEX.  DataHash is:" . Dumper($dataHashRef);
}

sub extractTables{
    my $content = shift @_;
    my $depth = shift @_;
    my $countArrayRef = shift @_;
    my $hashOfDataRef = shift @_;

    my @rowArray;

    #print "Starting extract of data from tables depth:$depth\n";
    my $te = HTML::TableExtract->new( depth => $depth);
    $te->parse($content);
    #print "Extracting data te is:" . Dumper($te) . "<<<---\n";
    foreach my $ts ( $te->tables ) {
        #print "Extracting data ts is:" . Dumper($ts) . "<<<---\n";
        foreach my $countWeWant (@$countArrayRef){
            #print "Extracting data from tables depth:$depth countWeWant:$countWeWant\n";
            if ($ts->count == $countWeWant){
                @rowArray = ();
                foreach my $row ( $ts->rows ) {
                    push(@rowArray, $row);
                }
                $hashOfDataRef->{"$depth-$countWeWant"} = [@rowArray];
            }
        }
    }
}

# FixMe: Need to fold this logic (from YahooEstimate) into the code below or create a new subroutine
#
#            $iRowCount=0;
#            foreach my $row ( $ts->rows() ){
#                if (0 == $iRowCount){
#                    $header = shift @$row;
#                } else {
#                    $label = shift @$row;
#                    s/,// for @$row;  #### strip commas from the fields
#                    if (   ("Earnings Est" eq $header) or ("Revenue Est" eq $header) or ("EPS Trends" eq $header) or ("EPS Revisions" eq $header)){
#                        for ($i = 0; $i <= 3; $i++) {
#                            $timePeriod = $timePeriods[$i];
#                            $key = "$timePeriod - $header - $label";
#                            if($dataWeWant{$key}){
#                                $data{$key} = $$row[$i];
#                            }
#                        }
#                    }
#                    if (   ("Growth Est" eq $header) ){
#                        $key = "$header - $label";
#                        if($dataWeWant{$key}){
#                            $data{$key} = $$row[0];
#                        }
#                    }
#                }
#                $iRowCount++;
#            }
#        }
#






### Strategic Solution...
sub extractDataKeyValuePairNEW{
    my $ticker = shift @_;
    my $tableDataHashRef = shift @_;
    my $dataHashRef = shift @_;
    my $dataName = shift @_;
    my $fieldsToGetHashRef = shift @_;

    ## KeysToHash contains the key which is 0-3 (where 0 is the depth and 3 is the count)
    my @keysToHash = (keys %$tableDataHashRef);
    ### YahooEstimates Specific

    my $header;
    if ($DEBUG > 3){
        print "This is the tableDataHashRef" . Dumper($tableDataHashRef) . "<<<<---\n";
        print "This is the fieldsToGetHashRef" . Dumper($fieldsToGetHashRef) . "<<<<---\n";
    }
    foreach my $keyToHash (@keysToHash){
        my $iRowCount=0;
        #print "keyToHash:$keyToHash iRowCount:$iRowCount\n";
        my @arrayOfArrayRefences = @{$tableDataHashRef->{$keyToHash}};
        foreach my $rowRef (@arrayOfArrayRefences){
            ##print "This is the rowRef:" . Dumper($rowRef) . "<<<<---\n";
            ## new process based on arrays to handle multiple values (and keys) for a given row
            my @keys;
            my $keysArrRef = \@keys;  #turn array into a reference so subroutines and can tweak it
            my @values;
            my $valuesArrRef = \@values;

            ### YahooEstimates Specific
            ### think this must be done before the shift of @$rowRef
            ###print "this is iRowCount:$iRowCount\n";
            if (('YahooEstimates' eq $dataName) and (0 == $iRowCount)) {
                  $header = shift @$rowRef;
                  #print "header:$header\n";
                  #print "This is the header:" . Dumper($header) . "<<<<---\n";
                  $iRowCount++;
                  next; #go to next record want/need to process the row
            }
            ## Get Keys and Values.. This is done after the Yahoo Specific code.
            ##    This is because the Yahoo Specific Code needs the whole row
            ##    Alternatively... this could be done before that code and header could be set to $$keysArrRef[0]
            push @$keysArrRef, shift @$rowRef;  ## Put key into Array Reference
            @$valuesArrRef = @$rowRef; ## put the remaining values (since key was removed) into the values array
            #print "\n--->>>keysArrRef:" . Dumper($keysArrRef) . "\n";
            #print "\n--->>>valuesArrRef:" . Dumper($valuesArrRef) . "\n";

            ## FixMe:  This is getting pretty ugly....
            if (('ZacksEstimates' eq $dataName) and ('0-9' eq $keyToHash)){
                $header = "Earnings Est";
                modifyKey($keysArrRef, $header);
            }

            if ( ('MarketWatchYrlyBalSheet' eq $dataName) or
                 ('MarketWatchYrlyIncStmt' eq $dataName) or
                 ('MarketWatchYrlyCashFlow' eq $dataName) ) {

                $header = "MWYrly";
                modifyKey($keysArrRef, $header);
                # print "\n--->>>keysArrRef is NOW:" . Dumper($keysArrRef) . "\n";
            }

            if ( ('MarketWatchQtrlyBalSheet' eq $dataName) or
                 ('MarketWatchQtrlyIncStmt' eq $dataName) or
                 ('MarketWatchQtrlyCashFlow' eq $dataName) ) {
                $header = "MWQtrly";
                modifyKey($keysArrRef, $header);
                # print "\n--->>>keysArrRef is NOW:" . Dumper($keysArrRef) . "\n";
            }

            if ( ('YahooEstimates' eq $dataName) and ($header) ) {
                  ##modify key
                  modifyKey($keysArrRef, $header);
                  ###print "\n--->>>keysArrRef is NOW:" . Dumper($keysArrRef) . "\n";
            }

            if ($DEBUG > 3) {print "keysArrRef:" . Dumper($keysArrRef) . "valuesArrRef:" . Dumper($valuesArrRef);}
            my $countOfItems = @$keysArrRef;
            # print "countOfItems:$countOfItems\n";
            ###foreach my $key (@$keysArrRef){
            my $i;
            for ($i = 0; $i < $countOfItems; $i++){
                my $key = $keysArrRef->[$i];
                my $value = $valuesArrRef->[$i];
              ###print "-------------------->keyToHash:$keyToHash key:$key value:$value\n";
              ## Matching criteria is either "ANY" or a specific depth-count combination
              foreach my $matchCriteria (keys %{$fieldsToGetHashRef}){
                      ## sort the hash keys by length descending to match longer names before shorter
                      ##    for example match Enterprise Value/Revenue before matchin on Enterprise Value
                      foreach my $mysearchkey (sort {length($b) <=> length($a)} keys %{$fieldsToGetHashRef->{$matchCriteria}}) {
                          # if (($mysearchkey =~ /rise Value/) and ($key =~ /Enter/)) {
                          my ($realName, $appendValue, $expansionNeeded, $exactMatch) =  @{$fieldsToGetHashRef->{$matchCriteria}{$mysearchkey}};
                          if ($DEBUG > 2) {
                              print "keyToHash:$keyToHash matching mysearchkey:$mysearchkey<- against key:$key<- value:$value exactMatch:$exactMatch\n";
                          }
                          if (  ( ( $key =~ m/^($mysearchkey)/) and (!($exactMatch)) ) or
                             ( ( $key =~ m/^($mysearchkey)$/) and ($exactMatch) )   ){
                              if ($DEBUG > 1) {print "\nFound Match!!! key:$key mysearchkey:$mysearchkey\n";}
                              $value =~ s/,//g;  ##FixMe: Is this always needed? Before it was only getting executed for field expansions

                              # my $realName;
                              # my $appendValue;
                              # my $expansionNeeded;
                              ##print "this is fieldsToGet" . Dumper($fieldsToGetHashRef->{$matchCriteria});
                              if ($realName){
                                  $key = $realName;  #replace value in key with the standard name
                              }
                              if ($appendValue){
                                  $value = $value . $appendValue;
                              }
                              if ($expansionNeeded){
                                  $value = expandValue($value);
                              }
                              if ($DEBUG > 1){print "ADDING TO HASH: ticker:$ticker dataName:$dataName key:$key value:$value\n";}
                              $dataHashRef->{$ticker}{$dataName}{$key} = $value;  ### use -> because its a hashRef
                              if ($DEBUG > 1) {print "This is the DataHashRef after the ADD-------->>>>" . Dumper($dataHashRef);}
                              last; ### exit loop for this row from the table because we found a match
                          }    ## ^matches at the beginning
                      } # end of foreach $mysearchkey
                    } # end of foreach $matchCriteria
            } # end of foreach $key
        }
    }
}

sub modifyKey{
  my $keysArrRef = shift @_;
  my $header = shift @_;

  my $key = shift @$keysArrRef; ### the calls to this should only have one key
  ### TRIM LEADING AND TRAILING WHITE SPACE
  $key =~ s/^\s+|\s+$//g;
  # the $keysArrRef should now be empty
  #print "header:$header\n";

  if (   ("Earnings Est" eq $header) or ("Revenue Est" eq $header) or ("EPS Trends" eq $header) or ("EPS Revisions" eq $header)){
        my @timePeriods = ("Curr Qtr","Next Qtr","Curr Yr","Next Yr");
        foreach my $timePeriod (@timePeriods){
        my $newKey = "$timePeriod-$header-$key";
        push @$keysArrRef, $newKey;
      }
  }
  #MarketWatch: Quarterly
  if ("MWQtrly" eq $header){
        my @timePeriods = ("Last Qtr-4", "Last Qtr-3", "Last Qtr-2", "Last Qtr-1", "Last Qtr");
        foreach my $timePeriod (@timePeriods){
        my $newKey = "$timePeriod-$key";
        push @$keysArrRef, $newKey;
      }
  }

  #MarketWatch: Yearly
  if ("MWYrly" eq $header){
        my @timePeriods = ("Last Yr-4","Last Yr-3", "Last Yr-2", "Last Yr-1", "Last Yr");
        foreach my $timePeriod (@timePeriods){
        my $newKey = "$timePeriod-$key";
        push @$keysArrRef, $newKey;
      }
  }

  if ("YrlyFinancials" eq $header){
        my @timePeriods = ("Last Yr","Last Yr - 1","Last Yr - 2","Last Yr - 3");
        foreach my $timePeriod (@timePeriods){
        my $newKey = "$timePeriod-$header-$key";
        push @$keysArrRef, $newKey;
      }
  }

  if (   ("Growth Est" eq $header) ){
     my $newKey  = "$header-$key";
     push @$keysArrRef, $newKey;
  }
}


# OLD VERSION
#sub extractDataKeyValuePair{
#    my $ticker = shift @_;
#    my $tableDataHashRef = shift @_;
#    my $keysArrRef = shift @_;
#    my $dataHashRef = shift @_;
#    my $dataName = shift @_;
#    my $fieldsToExpandRef = shift @_;
#
#    my %dataWeWant = map { $_ => 1 } @$keysArrRef;
#
#    my @keysToHash = (keys %$tableDataHashRef);
#    foreach my $keyToHash (@keysToHash){
#        my @arrayOfArrayRefences = @{$tableDataHashRef->{$keyToHash}};
#        foreach my $rowRef (@arrayOfArrayRefences){
#            my $key = shift @$rowRef;
#            my $value = shift @$rowRef;
#            foreach my $mysearchkey (@$keysArrRef) {
#                if ( $key =~ m/^($mysearchkey)/ ) {
#                    $key = $1;  #replace value in key with the standard name
#                    last;
#                }    ## ^matches at the beginning
#            }
#            if($dataWeWant{$key}){
#                $value =~ s/,//g;
#                if ($fieldsToExpandRef->{$key}){
#                    $value = expandValue($value);
#                }
#                $dataHashRef->{$ticker}{$dataName}{$key} = $value;  ### use -> because its a hashRef
#            }
#
#        }
#    }
#}
#
#

#strategic solution
sub extractRegEx{
    my $ticker = shift @_;
    my $dataName = shift @_;
    my $dataHashRef = shift @_;
    my $content = shift @_;
    my $regExHashRef = shift @_;

    foreach my $fieldName (keys %{$regExHashRef}){
        my $regExSearchString = $regExHashRef->{$fieldName};
        if ($DEBUG > 4){print "fieldName:$fieldName regExSearchString:$regExSearchString\n";}
        my $result = "";
        if ($content =~ m/$regExSearchString/) {
            if ($DEBUG > 4){
                print "extractRegEx Found A Match! fieldName:$fieldName value:$1<--\n";
            }
            $result=$1;
            #if ('ZacksIndustry' eq $dataName){$result=$2;}
            #FixMe: This is new so not sure if it will cause problems.. It's needed to strip commas from co description
            $result =~ s/,//g;
        }
        $dataHashRef->{$ticker}{$dataName}{$fieldName} = $result;
    }

}

sub getKeysWeWant{
    my $dataName = shift @_;

    my @keys;

    #### TABLE EXTRACT FIELDS
    my $fieldsWeWantFromTablesHashRef = getFieldsWeWantFromTables($dataName);

    #Match Criteria is "ANY" or specific depth and count combination (eg: 0-1)
    foreach my $matchCriteria (keys %$fieldsWeWantFromTablesHashRef) {
        ##print Dumper($fieldsWeWantFromTablesHashRef->{$matchCriteria});
        foreach my $mysearchkey (keys %{$fieldsWeWantFromTablesHashRef->{$matchCriteria}}) {
                my $realName;
                ($realName) =  @{$fieldsWeWantFromTablesHashRef->{$matchCriteria}{$mysearchkey}};
                #print "adding $realName\n";
                push (@keys, $realName);
        }
    }

    #### REGEX FIELDS
    my $fieldsWeWantViaRegExHashRef = getFieldsWeWantViaRegEx($dataName);

    foreach my $fieldName (keys %$fieldsWeWantViaRegExHashRef){
        push (@keys, $fieldName);
    }

    push (@keys, "LastModDt");

    my @sortedKeys = sort @keys;
    #print Dumper(@keys);

    return \@sortedKeys;

}


sub outputResults{
    my $outFH = shift @_;
    my $dataName = shift @_;
    my $keysArrRef = shift @_;
    my $resultsHashRef = shift @_;

    #write Headers
    print $outFH "ticker," . join(", ", @$keysArrRef) . "\n";

    foreach my $ticker (keys %$resultsHashRef){
        print $outFH $ticker;
        foreach my $fieldKey (@$keysArrRef){
            if ($resultsHashRef->{$ticker}{$dataName}{$fieldKey}){
                print $outFH "," . $resultsHashRef->{$ticker}{$dataName}{$fieldKey};
            } else {
                print $outFH ",0";
            }
        }
        print $outFH "\n";
    }
}


#####################
sub getStandardConfigs{
    my $dataName = shift @_;

    # These configs are standard and wont be impacted based on which computer is running this
    # The other configs (stored in % vary by definition)
    my %hash;

    # Zacks Industry
    $hash{"ZacksIndustry"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/Zacks/";
    $hash{"ZacksIndustry"}{"inFileSuffix"} = "-ZackIndustry.html";
    $hash{"ZacksIndustry"}{"outputFileName"} = "ALL-STOCKS-ZacksIndustry.csv";

    # Zacks Estimates
    $hash{"ZacksEstimates"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/Zacks/";
    $hash{"ZacksEstimates"}{"inFileSuffix"} = "-ZackEstimate.html";
    $hash{"ZacksEstimates"}{"outputFileName"} = "ALL-STOCKS-ZacksEstimate.csv";

    # Yahoo Industry
    $hash{"YahooIndustry"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/Yahoo/";
    $hash{"YahooIndustry"}{"inFileSuffix"} = "-Industry.html";
    $hash{"YahooIndustry"}{"outputFileName"} = "ALL-STOCKS-yahooIndustry.csv";

    # Yahoo Analyst
    $hash{"YahooAnalyst"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/Yahoo/";
    $hash{"YahooAnalyst"}{"inFileSuffix"} = "-Analyst.html";
    $hash{"YahooAnalyst"}{"outputFileName"} = "ALL-STOCKS-yahooAnalyst.csv";

    # Yahoo Key Stats
    $hash{"YahooKeyStats"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/Yahoo/";
    $hash{"YahooKeyStats"}{"inFileSuffix"} = "-KeyStats.html";
    $hash{"YahooKeyStats"}{"outputFileName"} = "ALL-STOCKS-yahooKeyStats.csv";

    # Yahoo Estimates
    $hash{"YahooEstimates"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/Yahoo/";
    $hash{"YahooEstimates"}{"inFileSuffix"} = "-Estimate.html";
    $hash{"YahooEstimates"}{"outputFileName"} = "ALL-STOCKS-yahooEstimates.csv";

    # MarketWatch Quarterly Balance Sheet
    $hash{"MarketWatchQtrlyBalSheet"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/MarketWatch/";
    $hash{"MarketWatchQtrlyBalSheet"}{"inFileSuffix"} = "-BalSheet.html";
    $hash{"MarketWatchQtrlyBalSheet"}{"outputFileName"} = "ALL-STOCKS-MktWtchQtrlyBalSheet.csv";

    # MarketWatch Yearly Balance Sheet
    $hash{"MarketWatchYrlyBalSheet"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/MarketWatch/";
    $hash{"MarketWatchYrlyBalSheet"}{"inFileSuffix"} = "-Yrly-BalSheet.html";
    $hash{"MarketWatchYrlyBalSheet"}{"outputFileName"} = "ALL-STOCKS-MktWtchYrlyBalSheet.csv";

    # MarketWatch Quarterly Income Statement
    $hash{"MarketWatchQtrlyIncStmt"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/MarketWatch/";
    $hash{"MarketWatchQtrlyIncStmt"}{"inFileSuffix"} = "-IncomeStmt.html";
    $hash{"MarketWatchQtrlyIncStmt"}{"outputFileName"} = "ALL-STOCKS-MktWtchQtrlyIncStmt.csv";

    # MarketWatch Yearly Income Statement
    $hash{"MarketWatchYrlyIncStmt"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/MarketWatch/";
    $hash{"MarketWatchYrlyIncStmt"}{"inFileSuffix"} = "-Yrly-IncomeStmt.html";
    $hash{"MarketWatchYrlyIncStmt"}{"outputFileName"} = "ALL-STOCKS-MktWtchYrlyIncStmt.csv";

    # MarketWatch Quarterly CashFlow Statement
    $hash{"MarketWatchQtrlyCashFlow"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/MarketWatch/";
    $hash{"MarketWatchQtrlyCashFlow"}{"inFileSuffix"} = "-CashFlow.html";
    $hash{"MarketWatchQtrlyCashFlow"}{"outputFileName"} = "ALL-STOCKS-MktWtchQtrlyCashFlow.csv";

    # MarketWatch Yearly CashFlow Statement
    $hash{"MarketWatchYrlyCashFlow"}{"inputPathSuffix"} = "Stocks/WebsiteDownloads/MarketWatch/";
    $hash{"MarketWatchYrlyCashFlow"}{"inFileSuffix"} = "-Yrly-CashFlow.html";
    $hash{"MarketWatchYrlyCashFlow"}{"outputFileName"} = "ALL-STOCKS-MktWtchYrlyCashFlow.csv";


    return \%{$hash{$dataName}};

}

sub getTableDepthsNeeded(){
    my $dataName = shift @_;

    ### Keys:
    ### 1st key: name of the DataName to allow matching
    ### 2nd key: count of the table(s) to target

    my %tableDepthCountsNeeded;
    $tableDepthCountsNeeded{"ZacksEstimates"}{0} = [0..15];
    $tableDepthCountsNeeded{"ZacksIndustry"}{0} = [2];
    $tableDepthCountsNeeded{"YahooIndustry"}{1} = [5];
    $tableDepthCountsNeeded{"YahooAnalyst"}{1} = [1..4];
    $tableDepthCountsNeeded{"YahooKeyStats"}{2} = [0..10];
    $tableDepthCountsNeeded{"YahooEstimates"}{2} = [0..6];

    $tableDepthCountsNeeded{"MarketWatchQtrlyBalSheet"}{0} = [0..3];
    $tableDepthCountsNeeded{"MarketWatchYrlyBalSheet"}{0} = [0..3];

    $tableDepthCountsNeeded{"MarketWatchQtrlyIncStmt"}{0} = [0..3];
    $tableDepthCountsNeeded{"MarketWatchYrlyIncStmt"}{0} = [0..3];

    $tableDepthCountsNeeded{"MarketWatchQtrlyCashFlow"}{0} = [0..3];
    $tableDepthCountsNeeded{"MarketWatchYrlyCashFlow"}{0} = [0..3];

    return \%{$tableDepthCountsNeeded{$dataName}};

}


sub getFieldsWeWantFromTables{
    my $dataName = shift @_;

    my %fieldsWeWantFromTables;
    ### 1st key: name of the DataName to allow matching
    ### 2nd key: table depth and count to target.. eg:  0-3 targets a table with a depth of 0 and count of 3.   "ANY" means match any
    ### 3rd key: name to match
    ### value: is an array reference that points to an array that holds:
    ###                 1) The final name of the field.. This allows you to translate names. Eg: match on DogName and tranlsate to: My Dogs Name
    ###                 2) The value to be appended to the field.  Eg:  "%" in this field will turn 7 into 7%
    ###                 3) Whether the field needs to be expanded
    ###                 4) Whether the match needs to be exact

    ### NOTE.... if the search string has parantheses.. they MUST be escaped!!!!

    ### ZacksIndustry
        #Nothing to get for this (CoDesc, Industry done in RegEx)

    ### ZacksEstimates
    #FixMe: Pull in other TABLES: Estimate trend table, Upside Table  <--- EARNINGS is DONE.. DO WE REALLY NEED OTHERS??
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Company Name'} = [('ZE CoName', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Report Date'} = [('ZE Next Report Date', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Current Qtr'} = [('ZE Grwth Curr Qtr', '%', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Current Year'} = [('ZE Grwth Curr Yr', '%', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Year'} = [('ZE Grwth Next Yr', '%', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Past 5 Years'} = [('ZE Grwth Last 5 Yrs', '%', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next 5 Years'} = [('ZE Grwth Next 5 Yrs', '%', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Qtr-Earnings Est-High Estimate'} = [('ZE Curr Qtr:Earnings Est:High Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Qtr-Earnings Est-High Estimate'} = [('ZE Next Qtr:Earnings Est:High Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Yr-Earnings Est-High Estimate'} = [('ZE Curr Yr:Earnings Est:High Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Yr-Earnings Est-High Estimate'} = [('ZE Next Yr:Earnings Est:High Est', '', 0)];

    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Qtr-Earnings Est-Low Estimate'} = [('ZE Curr Qtr:Earnings Est:Low Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Qtr-Earnings Est-Low Estimate'} = [('ZE Next Qtr:Earnings Est:Low Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Yr-Earnings Est-Low Estimate'} = [('ZE Curr Yr:Earnings Est:Low Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Yr-Earnings Est-Low Estimate'} = [('ZE Next Yr:Earnings Est:Low Est', '', 0)];

    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Qtr-Earnings Est-Most Recent Consensus'} = [('ZE Curr Qtr:Earnings Est:Most Recent Consensus Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Qtr-Earnings Est-Most Recent Consensus'} = [('ZE Next Qtr:Earnings Est:Most Recent Consensus Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Yr-Earnings Est-Most Recent Consensus'} = [('ZE Curr Yr:Earnings Est:Most Recent Consensus Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Yr-Earnings Est-Most Recent Consensus'} = [('ZE Next Yr:Earnings Est:Most Recent Consensus Est', '', 0)];

    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Qtr-Earnings Est-Zacks Consensus Estimate'} = [('ZE Curr Qtr:Earnings Est:Zacks Consensus Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Qtr-Earnings Est-Zacks Consensus Estimate'} = [('ZE Next Qtr:Earnings Est:Zacks Consensus Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Curr Yr-Earnings Est-Zacks Consensus Estimate'} = [('ZE Curr Yr:Earnings Est:Zacks Consensus Est', '', 0)];
    $fieldsWeWantFromTables{"ZacksEstimates"}{"ANY"}{'Next Yr-Earnings Est-Zacks Consensus Estimate'} = [('ZE Next Yr:Earnings Est:Zacks Consensus Est', '', 0)];

    ### Yahoo Industry
      # getting industry code via RegEx

    ### Yahoo Analysts
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'Mean Recommendation \(last week\)'} = [('YA Recommendation last week', '', 0)];
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'Mean Recommendation \(this week\)'} = [('YA Recommendation curr week', '', 0)];
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'Change'} = [('YA Recommendation Chg from last week', '', 0)];
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'Mean Target'} = [('YA Price Tgt (Mean)', '', 0)];
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'Median Target'} = [('YA Price Tgt (Median)', '', 0)];
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'High Target'} = [('YA Price Tgt (High)', '', 0)];
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'Low Target'} = [('YA Price Tgt (Low)', '', 0)];
    $fieldsWeWantFromTables{"YahooAnalyst"}{"ANY"}{'No. of Brokers'} = [('YA Price Tgt (Nbr Brokers)', '', 0)];


    ### Yahoo Keys Stats
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Trailing P/E'} = [('YKS Trailing P/E', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Forward P/E'} = [('YKS Forward P/E', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'PEG Ratio 5 yr expected'} = [('YKS PEG Ratio 5 yr expected', '', 0)];
    ## note must escape / in the search string
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Price\/Sales'} = [('YKS Price/Sales ttm', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Price\/Book'} = [('YKS Price/Book mrq', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Most Recent Quarter'} = [('YKS Most Recent Quarter', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Profit Margin'} = [('YKS Profit Margin', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Operating Margin'} = [('YKS Operating Margin', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Return on Assets'} = [('YKS Return on Assets', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Return on Equity'} = [('YKS Return on Equity', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Revenue \(ttm\)'} = [('YKS Revenue ttm', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Revenue Per Share'} = [('YKS Revenue Per Share', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Qtrly Revenue Growth'} = [('YKS Qtrly Revenue Growth', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Qtrly Earnings Growth'} = [('YKS Qtrly Earnings Growth yoy', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Total Cash \(mrq'} = [('YKS Total Cash mrq', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Total Cash Per Share'} = [('YKS Total Cash Per Share', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Operating Cash Flow'} = [('YKS Operating Cash Flow ttm', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Levered Free Cash Flow'} = [('YKS Levered Free Cash Flow', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Total Debt \(mrq'} = [('YKS Total Debt mrq', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Total Debt/Equity'} = [('YKS Total Debt/Equity', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Current Ratio'} = [('YKS Current Ratio', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Book Value Per Share'} = [('YKS Book Value Per Share', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Beta'} = [('YKS Beta', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Float'} = [('YKS Float', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'% Held by Insiders'} = [('YKS % Held by Insiders', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'% Held by Institutions'} = [('YKS % Held by Institutions', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Shares Short \(as'} = [('YKS Shares Short Most Recent', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Short Ratio'} = [('YKS Short Ratio', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Short % of Float'} = [('YKS Short % of Float', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Shares Short \(prior month'} = [('YKS Shares Short prior month', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Market Cap'} = [('YKS Market Cap', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Enterprise Value\/Revenue'} = [('YKS Enterprise Value/Revenue ttm', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Enterprise Value\/EBITDA'} = [('YKS Enterprise Value/EBITDA ttm', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Enterprise Value'} = [('YKS Enterprise Value', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Gross Profit'} = [('YKS Gross Profit ttm', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'EBITDA'} = [('YKS EBITDA ttm', '', 1)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Forward Annual Dividend Yield'} = [('YKS Forward Annual Dividend Yield', '', 0)];
    $fieldsWeWantFromTables{"YahooKeyStats"}{"ANY"}{'Payout Ratio'} = [('YKS Payout Ratio', '', 0)];

    # Yahoo Estimates
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Earnings Est-Avg. Estimate'} = [('YE Earnings Est:Avg Est:Curr Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Earnings Est-Avg. Estimate'} = [('YE Earnings Est:Avg Est:Next Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Earnings Est-Avg. Estimate'} = [('YE Earnings Est:Avg Est:Next Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Earnings Est-Avg. Estimate'} = [('YE Earnings Est:Avg Est:Curr Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Earnings Est-Low Estimate'} = [('YE Earnings Est:Low Est:Curr Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Earnings Est-Low Estimate'} = [('YE Earnings Est:Low Est:Next Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Earnings Est-Low Estimate'} = [('YE Earnings Est:Low Est:Next Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Earnings Est-Low Estimate'} = [('YE Earnings Est:Low Est:Curr Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Earnings Est-High Estimate'} = [('YE Earnings Est:High Est:Curr Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Earnings Est-High Estimate'} = [('YE Earnings Est:High Est:Next Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Earnings Est-High Estimate'} = [('YE Earnings Est:High Est:Next Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Earnings Est-High Estimate'} = [('YE Earnings Est:High Est:Curr Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Earnings Est-Year Ago EPS'} = [('YE Earnings Est:Year Ago EPS:Curr Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Earnings Est-Year Ago EPS'} = [('YE Earnings Est:Year Ago EPS:Next Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Earnings Est-Year Ago EPS'} = [('YE Earnings Est:Year Ago EPS:Next Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Earnings Est-Year Ago EPS'} = [('YE Earnings Est:Year Ago EPS:Curr Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Earnings Est-No. of Analysts'} = [('YE Earnings Est:No of Analysts:Curr Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Earnings Est-No. of Analysts'} = [('YE Earnings Est:No of Analysts:Next Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Earnings Est-No. of Analysts'} = [('YE Earnings Est:No of Analysts:Next Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Earnings Est-No. of Analysts'} = [('YE Earnings Est:No of Analysts:Curr Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Revenue Est-Avg. Estimate'} = [('YE Revenue Est:Avg Est:Curr Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Revenue Est-Avg. Estimate'} = [('YE Revenue Est:Avg Est:Next Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Revenue Est-Avg. Estimate'} = [('YE Revenue Est:Avg Est:Next Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Revenue Est-Avg. Estimate'} = [('YE Revenue Est:Avg Est:Curr Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Revenue Est-Low Estimate'} = [('YE Revenue Est:Low Est:Curr Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Revenue Est-Low Estimate'} = [('YE Revenue Est:Low Est:Next Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Revenue Est-Low Estimate'} = [('YE Revenue Est:Low Est:Next Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Revenue Est-Low Estimate'} = [('YE Revenue Est:Low Est:Curr Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Revenue Est-High Estimate'} = [('YE Revenue Est:High Est:Curr Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Revenue Est-High Estimate'} = [('YE Revenue Est:High Est:Next Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Revenue Est-High Estimate'} = [('YE Revenue Est:High Est:Next Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Revenue Est-High Estimate'} = [('YE Revenue Est:High Est:Curr Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Revenue Est-Year Ago Sales'} = [('YE Revenue Est:Year Ago Sales:Curr Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Revenue Est-Year Ago Sales'} = [('YE Revenue Est:Year Ago Sales:Next Qtr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Revenue Est-Year Ago Sales'} = [('YE Revenue Est:Year Ago Sales:Next Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Revenue Est-Year Ago Sales'} = [('YE Revenue Est:Year Ago Sales:Curr Yr','',1)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-Revenue Est-Sales Growth'} = [('YE Revenue Est:Sales Growth:Curr Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-Revenue Est-Sales Growth'} = [('YE Revenue Est:Sales Growth:Next Qtr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-Revenue Est-Sales Growth'} = [('YE Revenue Est:Sales Growth:Next Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-Revenue Est-Sales Growth'} = [('YE Revenue Est:Sales Growth:Curr Yr','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Trends-Current Estimate'} = [('YE EPS Trends:Curr Qtr:Current Est','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Trends-Current Estimate'} = [('YE EPS Trends:Next Qtr:Current Est','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Trends-Current Estimate'} = [('YE EPS Trends:Next Yr:Current Est','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Trends-Current Estimate'} = [('YE EPS Trends:Curr Yr:Current Est','',0)];
    ## changed 7 to 07 in final name to help with sorting
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Trends-7 Days Ago'} = [('YE EPS Trends:Curr Qtr:07 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Trends-7 Days Ago'} = [('YE EPS Trends:Next Qtr:07 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Trends-7 Days Ago'} = [('YE EPS Trends:Next Yr:07 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Trends-7 Days Ago'} = [('YE EPS Trends:Curr Yr:07 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Trends-30 Days Ago'} = [('YE EPS Trends:Curr Qtr:30 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Trends-30 Days Ago'} = [('YE EPS Trends:Next Qtr:30 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Trends-30 Days Ago'} = [('YE EPS Trends:Next Yr:30 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Trends-30 Days Ago'} = [('YE EPS Trends:Curr Yr:30 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Trends-60 Days Ago'} = [('YE EPS Trends:Curr Qtr:60 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Trends-60 Days Ago'} = [('YE EPS Trends:Next Qtr:60 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Trends-60 Days Ago'} = [('YE EPS Trends:Next Yr:60 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Trends-60 Days Ago'} = [('YE EPS Trends:Curr Yr:60 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Trends-90 Days Ago'} = [('YE EPS Trends:Curr Qtr:90 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Trends-90 Days Ago'} = [('YE EPS Trends:Next Qtr:90 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Trends-90 Days Ago'} = [('YE EPS Trends:Next Yr:90 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Trends-90 Days Ago'} = [('YE EPS Trends:Curr Yr:90 Days Ago','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Revisions-Up Last 7 Days'} = [('YE EPS Revisions:Curr Qtr:Up Last 7 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Revisions-Up Last 7 Days'} = [('YE EPS Revisions:Next Qtr:Up Last 7 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Revisions-Up Last 7 Days'} = [('YE EPS Revisions:Next Yr:Up Last 7 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Revisions-Up Last 7 Days'} = [('YE EPS Revisions:Curr Yr:Up Last 7 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Revisions-Up Last 30 Days'} = [('YE EPS Revisions:Curr Qtr:Up Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Revisions-Up Last 30 Days'} = [('YE EPS Revisions:Next Qtr:Up Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Revisions-Up Last 30 Days'} = [('YE EPS Revisions:Next Yr:Up Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Revisions-Up Last 30 Days'} = [('YE EPS Revisions:Curr Yr:Up Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Revisions-Down Last 30 Days'} = [('YE EPS Revisions:Curr Qtr:Down Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Revisions-Down Last 30 Days'} = [('YE EPS Revisions:Next Qtr:Down Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Revisions-Down Last 30 Days'} = [('YE EPS Revisions:Next Yr:Down Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Revisions-Down Last 30 Days'} = [('YE EPS Revisions:Curr Yr:Down Last 30 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Qtr-EPS Revisions-Down Last 90 Days'} = [('YE EPS Revisions:Curr Qtr:Down Last 90 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Qtr-EPS Revisions-Down Last 90 Days'} = [('YE EPS Revisions:Next Qtr:Down Last 90 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Next Yr-EPS Revisions-Down Last 90 Days'} = [('YE EPS Revisions:Next Yr:Down Last 90 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Curr Yr-EPS Revisions-Down Last 90 Days'} = [('YE EPS Revisions:Curr Yr:Down Last 90 Days','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-Current Qtr.'} = [('YE Growth Est:Curr Qtr.','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-Next Qtr.'} = [('YE Growth Est:Next Qtr.','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-This Year'} = [('YE Growth Est:This Year','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-Next Year'} = [('YE Growth Est:Next Year','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-Past 5 Years \(per annum\)'} = [('YE Growth Est:Past 5 Years (per annum)','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-Next 5 Years \(per annum\)'} = [('YE Growth Est:Next 5 Years (per annum)','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-Price/Earnings \(avg. for comparison categories\)'} = [('YE Growth Est:Price/Earnings (avg. for comparison categories)','',0)];
    $fieldsWeWantFromTables{'YahooEstimates'}{'ANY'}{'Growth Est-PEG Ratio \(avg. for comparison categories\)'} = [('YE Growth Est:PEG Ratio (avg. for comparison categories)','',0)];

### MarketWatch: BALANCE SHEET  (Quarterly and Yearly)

    # Retained Earnings
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{'Last Qtr-Retained Earnings'}   = [('MW Retained Earnings:Last Qtr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{'Last Qtr-1-Retained Earnings'} = [('MW Retained Earnings:Last Qtr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{'Last Qtr-2-Retained Earnings'} = [('MW Retained Earnings:Last Qtr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{'Last Qtr-3-Retained Earnings'} = [('MW Retained Earnings:Last Qtr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{'Last Qtr-4-Retained Earnings'} = [('MW Retained Earnings:Last Qtr-4', '', 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{'Last Yr-Retained Earnings'}   = [('MW Retained Earnings:Last Yr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{'Last Yr-1-Retained Earnings'} = [('MW Retained Earnings:Last Yr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{'Last Yr-2-Retained Earnings'} = [('MW Retained Earnings:Last Yr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{'Last Yr-3-Retained Earnings'} = [('MW Retained Earnings:Last Yr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{'Last Yr-4-Retained Earnings'} = [('MW Retained Earnings:Last Yr-4', '', 1)];

    # Shareholder Equity
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{"Last Qtr-Total Shareholders' Equity"}   = [('MW Total Shareholders Equity:Last Qtr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{"Last Qtr-1-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Qtr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{"Last Qtr-2-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Qtr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{"Last Qtr-3-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Qtr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyBalSheet"}{"ANY"}{"Last Qtr-4-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Qtr-4', '', 1, 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{"Last Yr-Total Shareholders' Equity"}   = [('MW Total Shareholders Equity:Last Yr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{"Last Yr-1-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Yr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{"Last Yr-2-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Yr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{"Last Yr-3-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Yr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyBalSheet"}{"ANY"}{"Last Yr-4-Total Shareholders' Equity"} = [('MW Total Shareholders Equity:Last Yr-4', '', 1, 1)];


### MarketWatch: INCOME STATEMENT  (Quarterly and Yearly)
    #EBITDA
    ### NOTE: Pass extra paramater = 1.. which forces a direct match... because there are several fields with the same name
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-EBITDA'}   = [('MW EBITDA:Last Qtr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-EBITDA'} = [('MW EBITDA:Last Qtr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-EBITDA'} = [('MW EBITDA:Last Qtr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-EBITDA'} = [('MW EBITDA:Last Qtr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-EBITDA'} = [('MW EBITDA:Last Qtr-4', '', 1, 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-EBITDA'}   = [('MW EBITDA:Last Yr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-EBITDA'} = [('MW EBITDA:Last Yr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-EBITDA'} = [('MW EBITDA:Last Yr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-EBITDA'} = [('MW EBITDA:Last Yr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-EBITDA'} = [('MW EBITDA:Last Yr-4', '', 1, 1)];

    #Interest Expense
    ### NOTE: Pass extra paramater = 1.. which forces a direct match... because there are several fields with the same name
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-Interest Expense'}   = [('MW Interest Expense:Last Qtr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-Interest Expense'} = [('MW Interest Expense:Last Qtr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-Interest Expense'} = [('MW Interest Expense:Last Qtr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-Interest Expense'} = [('MW Interest Expense:Last Qtr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-Interest Expense'} = [('MW Interest Expense:Last Qtr-4', '', 1, 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-Interest Expense'}   = [('MW Interest Expense:Last Yr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-Interest Expense'} = [('MW Interest Expense:Last Yr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-Interest Expense'} = [('MW Interest Expense:Last Yr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-Interest Expense'} = [('MW Interest Expense:Last Yr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-Interest Expense'} = [('MW Interest Expense:Last Yr-4', '', 1, 1)];


    #Depreciation & Amortization Expense
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-Depreciation & Amortization Expense'}   = [('MW Depreciation & Amortization Expense:Last Qtr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Qtr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Qtr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Qtr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Qtr-4', '', 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-Depreciation & Amortization Expense'}   = [('MW Depreciation & Amortization Expense:Last Yr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Yr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Yr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Yr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-Depreciation & Amortization Expense'} = [('MW Depreciation & Amortization Expense:Last Yr-4', '', 1)];

    #SG&A Expense
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-SG&A Expense'}   = [('MW SG&A Expense:Last Qtr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-SG&A Expense'} = [('MW SG&A Expense:Last Qtr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-SG&A Expense'} = [('MW SG&A Expense:Last Qtr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-SG&A Expense'} = [('MW SG&A Expense:Last Qtr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-SG&A Expense'} = [('MW SG&A Expense:Last Qtr-4', '', 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-SG&A Expense'}   = [('MW SG&A Expense:Last Yr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-SG&A Expense'} = [('MW SG&A Expense:Last Yr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-SG&A Expense'} = [('MW SG&A Expense:Last Yr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-SG&A Expense'} = [('MW SG&A Expense:Last Yr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-SG&A Expense'} = [('MW SG&A Expense:Last Yr-4', '', 1)];

    #Shares Outstanding
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-Basic Shares Outstanding'}   = [('MW Basic Shares Outstanding:Last Qtr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Qtr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Qtr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Qtr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Qtr-4', '', 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-Basic Shares Outstanding'}   = [('MW Basic Shares Outstanding:Last Yr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Yr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Yr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Yr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-Basic Shares Outstanding'} = [('MW Basic Shares Outstanding:Last Yr-4', '', 1)];


    #Gross Income
    ### NOTE: Pass extra paramater = 1.. which forces a direct match... because there are several fields with Gross Income embedded
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-Gross Income'}   = [('MW Gross Income:Last Qtr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-Gross Income'} = [('MW Gross Income:Last Qtr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-Gross Income'} = [('MW Gross Income:Last Qtr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-Gross Income'} = [('MW Gross Income:Last Qtr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-Gross Income'} = [('MW Gross Income:Last Qtr-4', '', 1, 1)];

    ### NOTE: Pass extra paramater = 1.. which forces a direct match... because there are several fields with Gross Income embedded
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-Gross Income'}   = [('MW Gross Income:Last Yr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-Gross Income'} = [('MW Gross Income:Last Yr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-Gross Income'} = [('MW Gross Income:Last Yr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-Gross Income'} = [('MW Gross Income:Last Yr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-Gross Income'} = [('MW Gross Income:Last Yr-4', '', 1, 1)];

    #Sales/Revenue
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-Sales\/Revenue'}   = [('MW Sales/Revenue:Last Qtr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-Sales\/Revenue'} = [('MW Sales/Revenue:Last Qtr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-Sales\/Revenue'} = [('MW Sales/Revenue:Last Qtr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-Sales\/Revenue'} = [('MW Sales/Revenue:Last Qtr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-Sales\/Revenue'} = [('MW Sales/Revenue:Last Qtr-4', '', 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-Sales\/Revenue'}   = [('MW Sales/Revenue:Last Yr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-Sales\/Revenue'} = [('MW Sales/Revenue:Last Yr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-Sales\/Revenue'} = [('MW Sales/Revenue:Last Yr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-Sales\/Revenue'} = [('MW Sales/Revenue:Last Yr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-Sales\/Revenue'} = [('MW Sales/Revenue:Last Yr-4', '', 1)];

    #Net Income
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-Net Income Available to Common'}   = [('MW Net Inc Avail to Common:Last Qtr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-1-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Qtr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-2-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Qtr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-3-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Qtr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyIncStmt"}{"ANY"}{'Last Qtr-4-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Qtr-4', '', 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-Net Income Available to Common'}   = [('MW Net Inc Avail to Common:Last Yr-0', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-1-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Yr-1', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-2-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Yr-2', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-3-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Yr-3', '', 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyIncStmt"}{"ANY"}{'Last Yr-4-Net Income Available to Common'} = [('MW Net Inc Avail to Common:Last Yr-4', '', 1)];


### MarketWatch: CASH FLOW   (Quarterly and Yearly)

    #Free Cash Flow
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-Free Cash Flow'}   = [('MW Free Cash Flow:Last Qtr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-1-Free Cash Flow'} = [('MW Free Cash Flow:Last Qtr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-2-Free Cash Flow'} = [('MW Free Cash Flow:Last Qtr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-3-Free Cash Flow'} = [('MW Free Cash Flow:Last Qtr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-4-Free Cash Flow'} = [('MW Free Cash Flow:Last Qtr-4', '', 1, 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-Free Cash Flow'}   = [('MW Free Cash Flow:Last Yr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-1-Free Cash Flow'} = [('MW Free Cash Flow:Last Yr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-2-Free Cash Flow'} = [('MW Free Cash Flow:Last Yr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-3-Free Cash Flow'} = [('MW Free Cash Flow:Last Yr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-4-Free Cash Flow'} = [('MW Free Cash Flow:Last Yr-4', '', 1, 1)];

    #Net Operating Cash Flow
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-Net Operating Cash Flow'}   = [('MW Net Operating Cash Flow:Last Qtr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-1-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Qtr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-2-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Qtr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-3-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Qtr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchQtrlyCashFlow"}{"ANY"}{'Last Qtr-4-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Qtr-4', '', 1, 1)];

    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-Net Operating Cash Flow'}   = [('MW Net Operating Cash Flow:Last Yr-0', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-1-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Yr-1', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-2-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Yr-2', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-3-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Yr-3', '', 1, 1)];
    $fieldsWeWantFromTables{"MarketWatchYrlyCashFlow"}{"ANY"}{'Last Yr-4-Net Operating Cash Flow'} = [('MW Net Operating Cash Flow:Last Yr-4', '', 1, 1)];


    return \%{$fieldsWeWantFromTables{$dataName}};
}


sub getFieldsWeWantViaRegEx{
    my $dataName = shift @_;
    my %regExSearches;

    ### ZacksIndustry

    ### ?: in the regex makes the group non-capturing
    $regExSearches{"ZacksIndustry"}{"ZI CoName"} = qr/(?:topbox_headline">|header>\s+<h1>)(.+?):/;
    ###print "\n\n\nSEARCH:$regExSearches{'ZacksIndustry'}{'ZI CoName'}\n";
    # $regExSearches{"ZacksIndustry"}{"ZI CoName"} = qr/header>\s+<h1>(.+?):/;   <span title="(.+?):
    ### OLD WAY:$regExSearches{"ZacksIndustry"}{"ZI CoDescription"} = qr/Company Description.+?<p>(.+?)<\/p>/;
    $regExSearches{"ZacksIndustry"}{"ZI CoDescription"} = qr/Company (?:Description|Summary).+?<p>(.+?)<\/p>/;

    ### ZacksEstimate
    $regExSearches{"ZacksEstimates"}{"ZE CompName"} = qr/<title>\w+:  (.+?) -/;
    $regExSearches{"ZacksEstimates"}{"ZE ValueRank"} = qr/Value: <span class=\"composite_val\">(\w+)</;
    $regExSearches{"ZacksEstimates"}{"ZE GrwthRank"} = qr/Growth: <span class=\"composite_val\">(\w+)</;
    $regExSearches{"ZacksEstimates"}{"ZE MomtmRank"} = qr/Momentum: <span class=\"composite_val\">(\w+)</;
    $regExSearches{"ZacksEstimates"}{"ZE IndstryRank"} = qr/ZACKS_QURD">(\d+) \/ \d+/;
    $regExSearches{"ZacksEstimates"}{"ZE RatingCode"} = qr/Zacks Rank : (\d)-\w+/;
    $regExSearches{"ZacksEstimates"}{"ZE RatingDesc"} = qr/Zacks Rank : \d-(\w+)/;

    ### Yahoo Estimates
    $regExSearches{"YahooEstimates"}{"YE Price"} = qr/yfs_l84_\S+">(\d+.\d+)/;

    ### Yahoo Industry
    $regExSearches{"YahooIndustry"}{"YI Industry"} = qr/Industry: (.+?)\|/;
    ### OLD WAY: $regExSearches{"YahooIndustry"}{"YI CoName"} = qr/href="\/q\/pr\?s=\w+">(.+?)<\/a>/;
    $regExSearches{"YahooIndustry"}{"YI CoName"} = qr/title"><h2>(.+?)\(/;



    return \%{$regExSearches{$dataName}};

}




#sub getZacksEstimateKeys{
#
#    my @keys = (
#    'Industry',
#    'Company Name',
#    'Company Description',
#    'Next Report Date',
#    'Current Qtr',
#    'Next Qtr',
#    'Current Year',
#    'Next Year',
#    'Past 5 Years',
#    'Next 5 Years',
#    );
#    return @keys;
#}
#
#sub getZackEstimateRegEx{
#    my %regExSearches;
#    $regExSearches{"CompName"} = qr/<header><h1>(\w+)/;
#    $regExSearches{"ValueRank"} = qr/Value: <span class=\"composite_val\">(\w+)</;
#    $regExSearches{"GrwthRank"} = qr/Growth: <span class=\"composite_val\">(\w+)</;
#    $regExSearches{"MomtmRank"} = qr/Momentum: <span class=\"composite_val\">(\w+)</;
#    $regExSearches{"IndstryRank"} = qr/ZACKS_QURD">(\d+) \/ \d+/;
#    $regExSearches{"RatingCode"} = qr/Zacks Rank : (\d)-\w+/;
#    $regExSearches{"RatingDesc"} = qr/Zacks Rank : \d-(\w+)/;
#    return \%regExSearches;
#}
#
#sub getYahooAnalystKeys{
#    my @keys = (
#    'Mean Recommendation this week',
#    'Mean Recommendation last week',
#    'Change',
#    'Mean Target',
#    'Median Target',
#    'High Target',
#    'Low Target',
#    'No. of Brokers');
#    return @keys;
#}
#sub getYahooKeyStatsKeys{
#    #### NOTE!!! - enterprise value must go after the enterprise value ratios... OR... EV will be matched 1st and you get the data in the wrong column
#    my @keys = (
#    'Trailing P/E', 'Forward P/E',         'PEG Ratio 5 yr expected',     'Price/Sales ttm',
#    'Price/Book mrq',   'Most Recent Quarter', 'Profit Margin', 'Operating Margin',
#    'Return on Assets', 'Return on Equity','Revenue ttm',
#    'Revenue Per Share',    'Qtrly Revenue Growth', 'Qtrly Earnings Growth yoy',
#    'Total Cash mrq','Total Cash Per Share', 'Operating Cash Flow ttm', 'Levered Free Cash Flow',
#    'Total Debt mrq', 'Total Debt/Equity',    'Current Ratio',
#    'Book Value Per Share', 'Beta', 'Float',
#    '% Held by Insiders',   '% Held by Institutions',
#    'Shares Short as', 'Short Ratio', 'Short % of Float', 'Shares Short prior month',
#    'Market Cap','Enterprise Value/Revenue ttm','Enterprise Value/EBITDA','Enterprise Value',
#    'Gross Profit ttm','EBITDA ttm','Forward Annual Dividend Yield',
#    'Payout Ratio');
#    return @keys;
#}
#
#sub getYahooKeyStatsFieldsToExpand{
#        my @number_keys = (
#        'Free Cashflow', 'Shares Short as', 'Shares Short prior month','Revenue ttm','Total Cash mrq',
#        'Total Debt mrq', 'Levered Free Cash Flow', 'Float','Shares Short as', 'Shares Short prior month',
#        'Market Cap','Enterprise Value','EBITDA ttm','Operating Cash Flow ttm','Gross Profit ttm'
#        );
#        return @number_keys;
#
#}

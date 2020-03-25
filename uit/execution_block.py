EXECUTION_BLOCK_TEMPLATE = '''
## Execution Block ----------------------------------------
echo -e "-------------------------\n"
echo -e "Starting Execute Block...\n"
echo -e "-------------------------\n"

# Configure Job/Run Dirs
JOBDIR=$PBS_O_WORKDIR
cd $JOBDIR

JOBID=`echo ${{PBS_JOBID}} | cut -d '.' -f 1 | cut -d '[' -f 1`

if [ -z ${{PBS_ARRAY_INDEX+x}} ]; 
  then :
  else
    RUNDIR=$JOBDIR/run_$PBS_ARRAY_INDEX
    mkdir -p ${{RUNDIR}}
    cd $RUNDIR
fi

# stage input data from archive
{archive_input_files}

# stage input data from home
{home_input_files}

## User Defined Script ----------------------------------------------
{execution_block}

## Cleanup ------------------------------------------------
# Cleanup is handled by a co-submitted script that has this script as its dependency.

echo -e "-------------------------\n"
echo -e "Finished Execute Block...\n"
echo -e "-------------------------\n"
'''

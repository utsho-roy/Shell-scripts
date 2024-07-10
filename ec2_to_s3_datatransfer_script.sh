#################################
#                                #
# This code sends file from ec2  #
# to s3 for processing           #
##################################


FILENAME=$1


function email() {
ATTACH_FILE=$1
SUBJECT="ERROR S3 FILE TRANSFER : See logs"
HOST=`hostname`
EMAIL_LIST1="HSDataEnggTeam@searshc.com"



if [[ $ATTACH_FILE == "" ]];
then
  echo "Error uploading file to S3" | mail -s "$SUBJECT" "$EMAIL_LIST1"
else
   mail -s "$SUBJECT" "$EMAIL_LIST1" < $ATTACH_FILE
fi
}

today=`date +%Y-%m-%d.%H:%M:%S`
todayfile=`date '+%Y%m%d'`
EMAIL_LIST="SOurceTeam@transformco.com"
EMAIL_LIST1="poc_email@transformco.com"
echo $todayfile


HUBtoS3_ERR_LOG_FILE="/var/app/pa_benefits_data/logs/transfer_pabenefits_file_to_s3_$FILENAME_$today.err"

NBR_FILES=$(find /var/app/pa_benefits_data/infilesFrompaBenfts/$FILENAME/ -name "*"|wc -l)

i=1

if [ $NBR_FILES -ne 0 ]; then

                for f in `find /var/app/pa_benefits_data/infilesFrompaBenfts/$FILENAME/ -name "*" -type f`
                do
                echo "file name is $f"
                echo "i value = $i"
                i=$((i+1));
                s3cmd put $f s3://shs-datahub-pa-benefits-data/$FILENAME/

                if [ $? -eq 0 ]
                then
                        echo "File $f successfully uploaded to S3" >> /var/app/pa_benefits_data/logs/transfer_pabenefits_file_to_s3_$FILENAME_$today.log
                        rm $f
                        echo "$f file successfully removed from hub " >> /var/app/pa_benefits_data/logs/transfer_pabenefits_file_to_s3_$FILENAME_$today.log
                else
                        echo "Error uploading this file to S3: $FILENAME " >> /var/app/pa_benefits_data/logs/transfer_pabenefits_file_to_s3_$FILENAME_$today.err
                        echo "Failed copying files to S3" >> /var/app/pa_benefits_data/logs/transfer_pabenefits_file_to_s3_$FILENAME_$today.log
                        # Email the error file
                        if [[ -s $HUBtoS3_ERR_LOG_FILE ]]; then
                            email "$HUBtoS3_ERR_LOG_FILE"
                        else
                            echo "Finished copying files to S3." >> /var/app/pa_benefits_data/logs/transfer_pabenefits_file_to_s3_$FILENAME_$today.log
                        fi

                        exit 1;
                fi

                done

else echo "No file found in $dir" >> /var/app/pa_benefits_data/logs/transfer_pabenefits_file_to_s3_$FILENAME_$today.log
     echo "PA Benefits file $f not received for $todayfile .  Please check and send the file." | mail -s "PA Benefits file $FILENAME for $todayfile not received on Datahub AWS server" "$EMAIL_LIST"  "$EMAIL_LIST1"
fi

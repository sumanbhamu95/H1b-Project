#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1)${MENU} Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2)${MENU} Find top 5 job titles who are having highest avg growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU}  Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} find top 5 locations in the US who have got certified visa for each year. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} Which top 5 employers file the most petitions each year? - Case Status - ALL ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year for all the applications${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year for only certified applications${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11)${MENU} Find the average Prevailing Wage for each Job for each Year ( full time). Arrange the output in descending order.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12)${MENU} Find the average Prevailing Wage for each Job for each Year (part time) . Arrange the output in descending order.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13)${MENU} Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed more than 1000) ?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 14)${MENU} 10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed more than 1000)?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 15)${MENU} Export result for question no 10 to MySql database.
${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
cd ~    
   hadoop jar q1a.jar /user/hive/warehouse/hiveh1b.db/h1b_final /user/hadoop/op1a ;
       hadoop fs -cat /user/hadoop/op1a/p*;        
       show_menu;
        ;;

        2) clear;
    
cd ~
   hadoop jar q1b.jar /user/hive/warehouse/hiveh1b.db/h1b_final /user/hadoop/op1b ;
       hadoop fs -cat /user/hadoop/op1b/p*;        
       show_menu;
        ;;
            
        3) clear;
cd ~
       hadoop jar q2a.jar /user/hive/warehouse/hiveh1b.db/h1b_final /user/hadoop/op2a ;
       hadoop fs -cat /user/hadoop/op2a/p*;        
       show_menu;
        ;;
	
        4) clear;
cd ~
          cd qsql;
         hive -f q2b.sql;
        show_menu;
        ;;
          5) clear;
cd ~
        cd qsql;
         hive -f q3.sql;
        show_menu;
        ;;
          6) clear;
cd ~
       cd qsql;
          hive -f q4.sql;
        show_menu;
        ;;
          7) clear;
	cd ~
         cd qpig
         pig q5a.pig
        show_menu;
        ;;
           8) clear;
 cd ~
       hadoop jar q5b.jar /user/hive/warehouse/hiveh1b.db/h1b_final /user/hadoop/op5b
        hadoop fs -cat /user/hadoop/op5b/p* 
        show_menu;
        ;;
           9) clear;
cd ~
        hadoop jar q6.jar /user/hive/warehouse/hiveh1b.db/h1b_final /user/hadoop/op6
        hadoop fs -cat /user/hadoop/op6/p*
        show_menu;
        ;;
           10) clear;
	cd ~;
         cd qsql;
         hive -f q7.sql
        show_menu;
        ;;
         11) clear;
	cd ~;
         cd qsql;
         hive -f q8a.sql
        show_menu;
        ;;
          12) clear;
        cd ~;
         cd qsql;
         hive -f q8b.sql
        show_menu;
        ;;
          13) clear;
	cd ~;
         cd qpig
         pig q9.pig
        show_menu;
        ;;
          14) clear;
cd ~
    hadoop jar q10.jar /user/hive/warehouse/hiveh1b.db/h1b_final /user/hadoop/op10 ;
       hadoop fs -cat /user/hadoop/op10/p*;        
       show_menu;
        
        ;; 
          15) clear;
        sqoop export --connect jdbc:mysql://localhost/suman --username root --password 'root' --table sumant --export-dir /user/hadoop/op10/part-r-00000 --input-fields-terminated-by '\t' ;
        show_menu;
        ;;  
	 


        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done



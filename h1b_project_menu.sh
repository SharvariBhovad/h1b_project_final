#!/bin/bash
show_menu()
{
	NORMAL=`echo "\033[m"`
	MENU=`echo "\033[36m"` #Blue
	NUMBER=`echo "\033[33m"` #yellow
	FGRED=`echo "\033[41m"`
	RED_TEXT=`echo "\033[31m"`
	ENTER_LINE=`echo "\033[33m"`

	echo -e "${MENU}*************APP MENU***************${NORMAL}"
	echo -e "${MENU}**${NUMBER} 1.a)${MENU} Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
	echo -e "${MENU}**${NUMBER} 1.b)${MENU} Find top 5 job titles who are having highest avg growth in applications.${NORMAL}"
	echo -e "${MENU}**${NUMBER} 2.a)${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
	echo -e "${MENU}**${NUMBER} 2.b)${MENU} find top 5 locations in the US who have got certified visa for each year. ${NORMAL}"
	echo -e "${MENU}**${NUMBER} 3)${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions? ${NORMAL}"
	echo -e "${MENU}**${NUMBER} 4)${MENU} Which top 5 employers file the most petitions each year? - Case Status - ALL${NORMAL}"
	echo -e "${MENU}**${NUMBER} 5.a)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?(for all the applications.) ${NORMAL}"
	echo -e "${MENU}**${NUMBER} 5.b)${MENU}  Find the most popular top 10 job positions for H1B visa applications for each year?(for only certified applications.)${NORMAL}"
	echo -e "${MENU}**${NUMBER} 6)${MENU} Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.${NORMAL}"
	echo -e "${MENU}**${NUMBER} 7)${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"
	echo -e "${MENU}**${NUMBER} 8)${MENU} Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order${NORMAL}"
	echo -e "${MENU}**${NUMBER} 9)${MENU} Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?${MENU} ${NORMAL}"
	echo -e "${MENU}**${NUMBER} 10)${MENU} Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?${MENU} ${NORMAL}"
	echo -e "${MENU}**${NUMBER} 11)${MENU} Export result for question no 10 to MySql database.
 ${NORMAL}"
	echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
	read opt
}
function option_picked()
{
	COLOR='\033[01;31m' #bold red
	RESET='\033[00;00m' #normal white
	MESSAGE="$1" #modified to post the correct option selected
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
		
		1.a) clear;
		option_picked "Is the number of petitions with Data Engineer job title increasing over time? ";
		rm /home/hduser/part-r-00000

		echo -e "Enter folder name to store file:"
		read folder_name
		 hadoop jar ques1ajar.jar ques1a.NoOfPetitionOfDE /user/hive/warehouse/h1b_project.db/h1b_final /h1b_project_output/$folder_name/
		 hadoop fs -get /h1b_project_output/$folder_name/
		xdg-open ~/$folder_name/part-r-00000
		show_menu;
		;;

		1.b) clear;
		option_picked "Find top 5 job titles who are having highest avg growth in applications.[ALL] ";
		show_menu;
		rm /home/hduser/part-r-00000

		echo -e "Enter folder name to store file:"
		read folder_name
		 hadoop jar ques1bjar.jar ques1b.Top5AvgGrowth  /user/hive/warehouse/h1b_project.db/h1b_final /h1b_project_output/$folder_name/
		 hadoop fs -get /h1b_project_output/$folder_name/
		show_menu;
		;;

		2.a) clear;
		option_picked "Which part of the US has the most Data Engineer jobs for each year?"
		mr-jobhistory-daemon.sh start historyserver
		hadoop fs -rm /h1b_project_output/ques_2a_data/*
		hadoop fs -rmdir /h1b_project_output/ques_2a_data
		hadoop fs -rm /h1b_project_output/ques_2a_output/*
		hadoop fs -rmdir /h1b_project_output/ques_2a_output

		./ques2a.sh
		hadoop fs -get /h1b_project_output/ques_2a_output/
		xdg-open ~/ques_2a_output/part-r-00000
		show_menu;
		;;

		2.b) clear;
		option_picked "find top 5 locations in the US who have got certified visa for each year."
		
		hadoop fs -rm /h1b_project_output/ques_2b_output*
		hadoop fs -rmdir /h1b_project_output/ques_2b_output

		mr-jobhistory-daemon.sh start historyserver
		pig -x mapreduce ques2bnew.pig

		hadoop fs -get /h1b_project_output/ques_2b_output/
 		xdg-open ~/ques_2b_output/part-r-00000
		show_menu;
		;;			
	
		3) clear;
		option_picked "Which industry(SOC_NAME) has the most number of Data Scientist positions?"
		echo -e "Enter folder name to store file:"
		read folder_name

		hive -hiveconf MY_VAR=$folder_name -f ques3.hql
		hadoop fs -get /h1b_project_output/$folder_name/
 		xdg-open ~/$folder_name/000000_0
		show_menu;
		;;	
			
		4) clear;
		option_picked "Which top 5 employers file the most petitions each year? - Case Status - ALL"
		mr-jobhistory-daemon.sh start historyserver
		echo -e "Enter folder name to store file:"
		read folder_name
		pig -param "filename=$folder_name" -x mapreduce /home/hduser/ques4.pig
		
		hadoop fs -get /h1b_project_output/$folder_name/
 		xdg-open ~/$folder_name/part-r-00000
		show_menu;
		;;	

		5.a) clear;
		option_picked "Find the most popular top 10 job positions for H1B visa applications for each year?(for all the applications)"
		echo -e "Enter folder name to store file:"
		read folder_name
		mr-jobhistory-daemon.sh start historyserver
		pig -param "filename=$folder_name" -x mapreduce /home/hduser/ques5a.pig
		hadoop fs -get /h1b_project_output/$folder_name/
 		xdg-open ~/$folder_name/part-r-00000	
		show_menu;
		;;	
		
		5.b) clear;
		option_picked "Find the most popular top 10 job positions for H1B visa applications for each year?(for only certified applications.)"
		echo -e "Enter the year(2011,2012,2013,2014,2015,2016)"
		read year
		echo -e "Enter folder name to store file:"
		read folder_name
		
		hive -hiveconf FOLDER_NAME=$folder_name -hiveconf VAR_YEAR=$year -f ques5b.hql

		hadoop fs -get /h1b_project_output/$folder_name/
 		xdg-open ~/$folder_name/000000_0
		show_menu;
		;;	
		
		6) clear;
		option_picked "Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time."
		echo -e "Enter folder name to store file:"
		read folder_name
		
		hive -hiveconf MY_VAR1=$folder_name -f ques6.hql
		hadoop fs -get /h1b_project_output/$folder_name/
 		xdg-open ~/$folder_name/000000_0
		show_menu;
		;;	

		7) clear;
		option_picked "Create a bar graph to depict the number of applications for each year [All]"
		echo -e "Enter folder name to store file:"
		read folder_name

		hive -hiveconf MY_VAR1=$folder_name -f ques7.hql
		hadoop fs -get /h1b_project_output/$folder_name/
 		xdg-open ~/$folder_name/000000_0
		
		show_menu;
		;;	

		8) clear;
		option_picked "Find the average Prevailing Wage for each Job for each Year"
		echo -e "Enter the year(2011,2012,2013,2014,2015,2016)"
		read year
		echo -e "Enter the choice Full time/ Part time.(Y/N)"
		read position
		echo -e "Enter folder name to store file:"
		read folder_name
	
		hive -hiveconf FOLDER_NAME=$folder_name -hiveconf MY_VAR1=$year -hiveconf MY_VAR2=$position -f ques8.hql
		show_menu;
		hadoop fs -get /h1b_project_output/$folder_name/
 		xdg-open ~/$folder_name/000000_0
		;;	

		9) clear;
		option_picked "Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?"
		rm /home/hduser/part-r-00000

		echo -e "Enter folder name to store file:"
		read folder_name
		 hadoop jar ques9jar.jar ques9.EmpSuccessRate /user/hive/warehouse/h1b_project.db/h1b_final /h1b_project/$folder_name/
		 hadoop fs -get /h1b_project/$folder_name/
		xdg-open ~/$folder_name/part-r-00000
		show_menu;
	
		;;	

		10) clear;
		option_picked "Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?"
		rm /home/hduser/part-r-00000
		echo -e "Enter folder name to store file:"
		read folder_name
		 hadoop jar ques10jar.jar ques10.jobPositionSuccessRate /user/hive/warehouse/h1b_project.db/h1b_final/ /h1b_project/$folder_name/
			
		 hadoop fs -get /h1b_project/$folder_name/
		xdg-open ~/$folder_name/part-r-00000
		show_menu;
		;;	

		11) clear;
		option_picked "Export result for question no 10 to MySql database."
		mr-jobhistory-daemon.sh start historyserver
		pig -x mapreduce /home/hduser/ques2a.pig
		show_menu;
		sqoop export --connect jdbc:mysql://localhost/h1b_project_mysql --username root --P --table ques11_output_table --export-dir /h1b_project_output/ques10a/part-r-00000 --input-fields-terminated-by ‘\t’ -m 1
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

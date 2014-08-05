switch(type) {
case 0:
    if(text == ' ')
	key = user.concat('1');
    else
	key = user.concat('0');

    cell.setAttribute('sorttable_customkey', key);
    break;
case 1:
    if(text == ' ')
	key = project.concat(user).concat('1');
    else
	key = project.concat(user).concat('0');

    cell.setAttribute('sorttable_customkey', key);
    break;
case 2:
    if(text == 'Project Total')
	key = user.concat(project).concat('0');
    else
	key = user.concat(project).concat(site);

    cell.setAttribute('sorttable_customkey', key);
    break;
case 3:
    totJobs = totJobs.toString();
    curJobs = curJobs.toString();

    if(site == 'Project Total')
	key = totJobs.concat(user).concat(totJobs);
    else
	key = totJobs.concat(user).concat(curJobs);
    cell.setAttribute('sorttable_customekey', key);
    break;
case 4:
    break;
case 5:
    break;
case 6:
    break;
}

# nse-bhavopy-load

Setting up a Ubuntu Server

  Installing python
    > sudo apt-get update
    > sudo apt-get install python

  Installing python packages
    > sudo apt-get install python-mysqldb

  Installing pip & click
    > sudo apt install python-pip
    > sudo pip install click

  if installing click throws an error with locales settings
    > export LC_ALL="en_US.UTF-8"
    > export LC_CTYPE="en_US.UTF-8"
    > sudo dpkg-reconfigure locales

  save the settings and then retry to install click using pip


Get the repo from github
  > git clone https://github.com/atha-digital-labs/nse-bhavopy-load.git
  > cd nse-bhavopy-load
  > mkdir log
  > mkdir tmp

Pepare mySQL Database using scripts in SQL Folder

To start loading data use the script load-bhavcopy.py

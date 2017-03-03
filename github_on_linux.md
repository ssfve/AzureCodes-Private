#Github Installation
First, make sure there is a hidden folder under your home directory (i.e. /home/yinlin) called .ssh

If there is not such a folder, make one using the command below
> mkdir .ssh

Go into the folder
>cd .ssh

Type the command below in the shell
>ssh-keygen -t rsa

Press enter until it finishes, accepting all default settings
This will create a file call id_rsa and a public key called id_rsa.pub

then put the key in authorized_keys
>cat id_rsa.pub >> authorized_keys

Open authorized_keys, copy the key you just created.

A Sample may be like this (a whole line, no carrier return, no parentheses):

(ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEArfSS+B04HTwrtnoFIHKbbI68KKpK77nyLtMFFZlwVOcb2hEz2O8PLjZzwqHYK2969+3VbOQtM+
ne37eV0O7iF9BmqY4fYzmra1sfYHwEVgytMzAnCFO35QT7RK7cPqNY2xD69ivYOsdqLN4CTRAs0bqIzU/13b3jB8FLixCizShJONo05uHDEQey
F9nDhnyhRdnMfhkH4VtbbwUwmTAZ2muuq0iIOcSB9UYAhSdZHFEM7dbDHRy3nXIAqPW7AbyVAYQAO4OYvDV2CYtrmeTijR32bswUjXx9qJ9Cpj
5CjngPg7VcRca1PEM1YMELWFziAgEFauZtAl90LzGfxd7A8Q== yinlin@c9t26359.itcs.hpecorp.net)

Paste this into your https://github.hpe.com account Settings from the web upper right corner    
(Settings->SSH and GPG keys->New SSH key)  
then back to your linux servers type the command below in the shell
>ssh -T git@github.hpe.com

Input yes when needed
If you see the message below, then you are connected
>Hi <your-username>! You've successfully authenticated, but GitHub does not provide shell access.

After that you can do anything you want with your git commands.  
Keep in mind that you are using ssh to communicate with github cloud servers.  
Return to your home directory  
>cd /home/yinlin  

Download cloud repository to your home directory like this  
>git clone git@github.hpe.com:RadarITR2POC/NewTechPOC.git  

The above steps shall let you get all files on github to your linux

#Github Utility
Add an alias for the address like this
>git remote add NewTechPOC git@github.hpe.com:RadarITR2POC/NewTechPOC.git

Config your email (don't use mine)
>git config --global user.email yin.lin@hpe.com

Config your username
>git config --global user.name yin-lin

Add tracking to a directory (not tracked before) and all the files in it
>git add conf/ITR2/

Add tracking to new file in a directory (tracked already)
>git add conf/ITR2/example.txt

Automatically commit changes and add comments to this change
>git commit -m “comments” -a

Finally, and the most used commands

Update your local directory
>git pull

After a commit to your local directory, you can push this repository to the cloud side master branch
>git push -u NewTechPOC master

The above steps shall let you push files from your linux to github

#How to Stash my Code? 
If you are changing your code in your local github repository,

But before finishing you want to pull from github again, then it is time to use
>git stash

use this to show a list of stashed code
>git stash list

use this to load your stashed code of num back to local repository
>git stash pop stash@{num}

#Github Management
use the command below to see a list of tracked and untracked files
>git status

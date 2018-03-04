#!/bin/bash

mkdir cnn_classify
cd cnn_classify
git init
dvc init
dvc config AWS.StoragePath dvc-share/dvc_tutorial
git add .
git commit -m 'Init DVC'

mkdir data
cp ../deeppy/train.zip data/
dvc add data/train.zip
git add data/
git status
#On branch master
#Changes to be committed:
#  (use "git reset HEAD <file>..." to unstage)
#
#    new file:   data/.gitignore
#    new file:   data/train.zip.dvc
git commit -m 'Raw data'

dvc run -d data/train.zip -o data/train unzip -d data/ data/train.zip
git status
#On branch master
#Changes to be committed:
#  (use "git reset HEAD <file>..." to unstage)
#  (use "git checkout -- <file>..." to discard changes in working directory)
#
#    modified:   data/.gitignore
#
#Untracked files:
#  (use "git add <file>..." to include in what will be committed)
#    new file:   train.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

git add data/.gitignore train.dvc
git commit -m 'Unzip files'

mkdir code
vi code/process_files.py
vi code/conf.py
dvc run -d code/process_files.py -d data/train -o data/cats_and_dogs_small -f process.dvc python code/process_files.py
git status
#On branch master
#Changes not staged for commit:
#  (use "git add <file>..." to update what will be committed)
#    (use "git checkout -- <file>..." to discard changes in working directory)
#
#        modified:   data/.gitignore
#
#Untracked files:
#  (use "git add <file>..." to include in what will be committed)
#
#        code/
#        process.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

echo '__pycache__/' >> code/.gitignore
echo '*.pyc' >> code/.gitignore
git add .
git commit -m 'Process raw data'

vi code/model.py
dvc run -d code/model.py -d code/conf.py -d data/cats_and_dogs_small -o data/model.h5 -o data/history.p python code/model.py
git status
#On branch master
#Changes not staged for commit:
#  (use "git add <file>..." to update what will be committed)
#    (use "git checkout -- <file>..." to discard changes in working directory)
#
#        modified:   data/.gitignore
#
#Untracked files:
#  (use "git add <file>..." to include in what will be committed)
#
#        code/model.py
#        model.h5.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

git add .
git commit -m 'First model'

vi code/plot.py
dvc run -d code/conf.py -d code/plot.py -d data/history.p -o data/plot_loss.jpeg -o data/plot_acc.jpeg python code/plot.py

dvc run -d data/plot_loss.jpeg

git status
#On branch master
#Changes not staged for commit:
#  (use "git add <file>..." to update what will be committed)
#    (use "git checkout -- <file>..." to discard changes in working directory)
#
#        modified:   data/.gitignore
#
#Untracked files:
#  (use "git add <file>..." to include in what will be committed)
#
#        Dvcfile
#        code/plot.py
#        plot_loss.jpeg.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

git add .
git commit -m 'Accuracy and loss plots'

git tag -a v0.1-first-cnn -m 'First CNN model'


############################################ v2
vi code/model.py
# Actual changes: augmentation, Dropout=0.5, batch_size=20-->32, epochs=30-->100
dvc repro

git status
#On branch master
#Changes not staged for commit:
#  (use "git add <file>..." to update what will be committed)
#    (use "git checkout -- <file>..." to discard changes in working directory)
#
#        modified:   Dvcfile
#        modified:   code/model.py
#        modified:   model.h5.dvc
#        modified:   plot_loss.jpeg.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

git add .
git commit -m 'Retrain augmentation model'
git tag -a v0.2-augm-cnn -m 'CNN model with augmentation'

########################################### v3

git checkout v0.1-first-cnn -b pre_trained
dvc checkout
vi code/model.py
dvc repro
git status
#On branch pre_trained
#Changes not staged for commit:
#  (use "git add <file>..." to update what will be committed)
#    (use "git checkout -- <file>..." to discard changes in working directory)
#
#        modified:   Dvcfile
#        modified:   code/model.py
#        modified:   model.h5.dvc
#        modified:   plot_loss.jpeg.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

git add .
git commit -m 'Pretrained VGG16'
git tag -a v0.3-vgg16_base -, 'Pretrained VGG16 tag'


############################################### v4

git checkout v0.2-augm-cnn -b vgg16_augm
dvc checkout
vi code/model.py
dvc repro
git status
#On branch vgg16_augm
#Changes not staged for commit:
#  (use "git add <file>..." to update what will be committed)
#    (use "git checkout -- <file>..." to discard changes in working directory)
#
#        modified:   Dvcfile
#        modified:   code/model.py
#        modified:   model.h5.dvc
#        modified:   plot_loss.jpeg.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

git add .
git commit -m 'VGG16 with augm'
git tag -a v0.4-vgg16_augm -m 'VGG16 with augm tag'

################################################# v5

vi code/model.py
dvc repro
git status
#On branch vgg16_augm
#Changes not staged for commit:
#  (use "git add <file>..." to update what will be committed)
#    (use "git checkout -- <file>..." to discard changes in working directory)
#
#        modified:   Dvcfile
#        modified:   code/model.py
#        modified:   model.h5.dvc
#        modified:   plot_loss.jpeg.dvc
#
#no changes added to commit (use "git add" and/or "git commit -a")

git add .
git commit -m 'VGG, augmentation and fine-tuning'
git tag -a v0.5-vgg_augm_fine -m 'VGG, augmentation and fine-tuning tag'

git checkout master
git merge vgg16_augm

git remote add origin https://github.com/dmpetrov/cnn_classify.git
git push -u origin master --tags

git push -u origin pre_trained
git push -u origin vgg16_augm

dvc cache push


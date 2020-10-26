# Installation

The easiest way to install ep3 is to use `pip` (here we show the install inside of a conda environment):

``` bash
conda create -n ep3 python=3.7 pip
conda activate ep3
pip install ep3
```

Or you can clone the [github repo](https://github.com/thespacedoctor/ep3) and install from a local version of the code:

``` bash
git clone git@github.com:thespacedoctor/ep3.git
cd ep3
python setup.py install
```

To upgrade to the latest version of ep3 use the command:

``` bash
pip install ep3 --upgrade
```

To check installation was successful run `ep3 -v`. This should return the version number of the install.

## Development

If you want to tinker with the code, then install in development mode. This means you can modify the code from your cloned repo:

``` bash
git clone git@github.com:thespacedoctor/ep3.git
cd ep3
python setup.py develop
```

[Pull requests](https://github.com/thespacedoctor/ep3/pulls) are welcomed! 

<!-- ### Sublime Snippets

If you use [Sublime Text](https://www.sublimetext.com/) as your code editor, and you're planning to develop your own python code with soxspipe, you might find [my Sublime Snippets](https://github.com/thespacedoctor/ep3-Sublime-Snippets) useful. -->



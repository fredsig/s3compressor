# s3compressor

Although storage pricing is cheap, using S3 Lifecycle to transition millions of small-sized objects into Glacier for long-term archiving will incur into considerable costs. s3compressor downloads all objects for each day, compress them into a zip file that will be uploaded to an archive S3 bucket using Glacier has a storage class and if successful, it will then delete temporary local files and source S3 objects. Example of the object path structure on the source bucket:

```
sources/2017/02/10/0/6/my_file.xml
sources/2017/02/11/1/2/my_file.xml
```

The default prefix argument is ``sources`` and it can be changed easily. The same can be said for the path structure, although the code has to be modified.

It makes use of Python threads for faster i/o on object downloads and deletes. It also uses boto3's multipart upload and concurrency. You might want to adjust the config for your own use case.
Compression is made using zip64, which is single process/core.

## Requirements
Requires Python 3 and [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) as specified in ```requirements.txt```.

## Installation
If you have [pyenv](https://github.com/pyenv/pyenv) and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) installed:

```make pyenv```

## Running

For best cost/performance, I recommend running it inside the VPC (via EC2 instance or ECS task) with the appropriate S3 endpoint configured or data transfer charges could add up.

```python s3compress.py -h``` will give you all available options.

To compress all objects for 2018 from source-bucket to archive-bucket using 16 threads:

```python s3compress.py --arch_bucket archive-bucket --bucket source-bucket --years 2018 --threads 16```

Local data directory points to /tmp but if you need more temporary space for download and compression change ```data_dir``` to something else.

## Warning

Although s3compressor has been used to successfully archive hundreds of millions of objects into Glacier, use it at your own risk. Source objects should only be deleted if we successfully download/compress them and upload the archive. To prevent any sort of data loss, you could easily comment the call to the delete_archive() function.

## License

Licensed under the [MIT license](http://www.opensource.org/licenses/mit-license.php).

> Copyright (c) 2019 Frederico Marques
> 
> Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
> 
> The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
> 
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

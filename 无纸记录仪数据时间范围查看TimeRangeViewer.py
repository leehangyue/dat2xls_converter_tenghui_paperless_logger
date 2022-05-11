"""
辉控THW480K无纸记录仪数据导出工具
读取二进制.DAT文件，导出通用文本文件
Dedecated data converter for DAQ (无纸记录仪) THW480K (辉控)
Read binary .DAT file and export text files
"""

from calendar import timegm
from io import FileIO
from os.path import getsize, split, splitext, join, exists, isfile, isdir
from os import listdir, mkdir, system
from time import strptime, strftime, gmtime

class DataReader:
    def __init__(self) -> None:
        self.ncol:int = 0
        self.decpos:list[int] = []  # decimal position
        self.units:list[str] = []
        self._src:FileIO = None  # IO interface from the built-in method 'open'
        self.fmt:DataBinFormat = DataBinFormat()
        self.header:bytes = []  # file header
        self.fsize:int = 0  # file size in byte

    def __str__(self) -> str:
        string = '通道号\t小数位数\t单位\n'
        for i in range(self.ncol):
            try:
                decpos = str(self.decpos[i])
            except IndexError:
                decpos = ''
            try:
                unit = self.units[i]
            except IndexError:
                unit = ''
            string += 'CH'+str(i)+'\t'+decpos+'\t'+unit+'\n'
        return string

    def from_str(self, string:str):
        while '  ' in string:
            string = string.replace('  ', ' ')
        string = string.replace('\r', '\n')
        while '\n\n' in string:
            string = string.replace('\n\n', '\n')
        lines = string.replace(' ', '\t').split('\n')
        # assert lines[0] == '通道号\t小数位数\t单位'
        lines = lines[1:]
        self.decpos = []
        self.units = []
        for i, line in enumerate(lines):
            if not line:
                continue
            entries = line.split('\t')
            # assert int(entries[0].strip('CH')) == i + 1
            self.decpos.append(int(entries[1]))
            self.units.append(entries[2])
        self.ncol = len(self.decpos)

    def load_settings(self, settings_file:str=''):
        if not settings_file:
            settings_file = join(split(__file__)[0], 'DataReaderDefaultSettings.txt')
        if not exists(settings_file):
            self.save_settings(settings_file)
            return
        with open(settings_file, 'rb') as f:
            self.from_str(f.read().decode('utf8', 'ignore'))

    def save_settings(self, settings_file:str=''):
        if not settings_file:
            settings_file = join(split(__file__)[0], 'DataReaderDefaultSettings.txt')
        settings_file_dir = split(settings_file)[0]
        if not exists(settings_file_dir):
            mkdir(settings_file_dir)
        with open(settings_file, 'w') as f:
            f.write(str(self))

    @property
    def src(self) -> str:
        return self._src.name

    @src.setter
    def src(self, src_filename:str):
        self._src = open(src_filename, 'rb')
        self.header = self._src.read(self.fmt.len_header)
        self.fsize = getsize(src_filename)

    def progress(self) -> float:
        ratio = self._src.tell() / self.fsize
        return ratio

    def get_timestamps(self, nlines:int=2**30) -> list:
        data = []
        # len(data) == nlines, len(data[i]) == self.ncol + 1,  
        # data[i][0]:int == a timestamp (epoch time), data[i][j>=1]:int == a data entry
        for _i in range(nlines):
            timestamp = int.from_bytes(self._src.read(self.fmt.len_timestamp), byteorder='big', 
                                       signed=False) + self.fmt.timeorigin
            self._src.seek(self.fmt.len_dataentry * self.ncol, 1)
            if self._src.tell() >= self.fsize:
                break
            else:
                data.append(timestamp)
        return data

    def get_titles(self):
        titles = ['Time', ]
        for i in range(self.ncol):
            title = 'CH' + str(i + 1) + '[' + self.units[i] + ']'
            titles.append(title)
        return titles

    def close(self):
        self._src.close()

class DataBinFormat:
    def __init__(self) -> None:
        self.len_header:int = 3  # bytes
        self.timeorigin = timegm(strptime('2000/01/01', '%Y/%m/%d'))
        self.len_timestamp:int = 4  # bytes, int32 seconds from self.timeorigin
        self.len_dataentry:int = 2  # bytes, int16

def show_time_range(in_file:str, settings_file:str=''):
    if not in_file:
        raise ValueError("Input file not specified.")
    in_file = in_file.strip('&').strip(' ').strip('\'').strip('\"')
    reader = DataReader()
    reader.src = in_file
    if not settings_file:
        settings_file = splitext(in_file)[0] + '_settings.txt'
    else:
        settings_file = settings_file.strip('&').strip(' ').strip('\'').strip('\"')
    reader.load_settings(settings_file)
    timestamps = reader.get_timestamps(2**30)
    print('File:\t', split(in_file)[1])
    print('From:\t', strftime('%Y/%m/%d %H:%M:%S', gmtime(timestamps[0])), 
          '\tTo:\t', strftime('%Y/%m/%d %H:%M:%S', gmtime(timestamps[-1])))
    reader.close()

if __name__ == '__main__':
    print(__doc__)
    print("更新于 Last updated on 2022-5-11\n")
    in_file = input("\n请把要转化的无纸记录仪数据（*.DAT）或其所在文件夹拖到此处：\nPlease drag-and-drop the data file (*.dat) or its parent folder here:\n").strip('&').strip(' ').strip('\'').strip('\"')
    settings_file = input("\n请把数据格式设定文件拖到此处：\nPlease drag-and-drop the settings file here:\n").strip('&').strip(' ').strip('\'').strip('\"')
    if isfile(in_file):
        file = in_file
        try:
            show_time_range(file, settings_file)
        except Exception:
            print("未能成功查看，请检查输入文件和设定文件，或联系开发者\n\n错误信息：\n\nFailed viewing. Please examine the input file and the settings file, or contact the developer\n\nError message:\n")
            import traceback
            traceback.print_exc()
            pass
    elif isdir(in_file):
        filelist = listdir(in_file)
        filelist = [join(in_file, file) for file in filelist if 'DAT' in splitext(file)[1].upper()]
        for file in filelist:
            try:
                show_time_range(file, settings_file)
            except Exception:
                print("未能成功查看，请检查输入文件和设定文件，或联系开发者\n\n错误信息：\n\nFailed viewing. Please examine the input file and the settings file, or contact the developer\n\nError message:\n")
                import traceback
                traceback.print_exc()
                pass
    system("pause")

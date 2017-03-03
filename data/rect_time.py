import os
import time
import glob
import logging
import logging.config
import argparse
import sys
import json

def handle_commandline():
    """Do with script's arguments
        Args:
            datafile_path
    Returns:
        dict(datafile_path)
    """
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-p", "--path", type=str,
                        help="the datafile_path where the datafiles are stored",
                        required=True)
    
    args = parser.parse_args()
    argsdict = vars(args)
    return argsdict

def parse_json_file(old_file_path, diff_time):
    """Parse file_name_path in JSON and replace "userID" with user
    Args:
        file_name_path (str): absolute path and file name
    Returns:
        mod_data_lines
    """
    mod_data_lines = list()
    f_in = open(old_file_path, "r")
    for line in f_in.readlines():
        file_dict = json.loads(line.strip("null"))
	file_dict["timestamp"] = file_dict["timestamp"] + diff_time
	mod_data_lines.append(json.dumps(file_dict))
    f_in.close()

    return mod_data_lines

def get_files(in_path_file, logger):
    """Get files to be processed for arguments and return a list of files to be processed
    Args:
        in_path_file (str): file directory or files with wild character like "?" "*" or a single file which is to
        be processed.
        logger (logger object): logger object
    Returns:
        list of full path of each data file
    """

    # make sure there is no "\" or "/" appended to directory
    path_file = in_path_file.rstrip("\\").rstrip('/')
    # Check if DATA_PATH exists or not. If not, exit with error singal
    if not os.path.exists(path_file):
        if len(glob.glob(path_file)) == 0:
            # print("The DATA directory or file %s DOES NOT exist and exit abnormally!" % in_path_file)
            logger.error("The DATA directory or file %s DOES NOT exist and exit abnormally!" % in_path_file)
            sys.exit(1)
        else:
            # Means there are files match in_file_path if wild charaters are used. C;\\*.sql
            return glob.glob(path_file)
    elif len(glob.glob(path_file)) == 1 and not os.path.isdir(path_file):
        # Means path_file is an existing file
        return glob.glob(path_file)
    elif len(os.listdir(path_file)) == 0:
        # Means in_path_file is an empty directory.
        logger.debug("The directory %s is an empty directory, and EXIST!" % in_path_file)
        sys.exit(0)
    else:
        # path_file is an existing directory and consists of several files
        file_list = os.listdir(path_file)
        # print file_list
        # Sort files in timestamp in file_name like 20160223171929_heartRate.
        # to be processed.
        file_list.sort(key=lambda x: x.split('_')[0])
        # print file_list
        return [os.path.join(path_file, ff) for ff in file_list]

# Main part
# Create NB_USERS threads one y one in sequence with an random (poisson) sleep time
def main():

    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)-5s] (%(threadName)-28s) %(message)-20s', )

    # Handle arguments
    args_dict = handle_commandline()
    logging.debug("    %s", args_dict)

    # Get files to be processed
    logging.info("==> Get files to be processed ...")
    
    files_2_processed = get_files(args_dict["path"], logging)
    
    logging.debug("    %s", files_2_processed)

    # Parse files to be processed
    logging.info("==> Parse files to be processed ...")
    # when parsing change the timestamp

    mod_datalines = list()
    for file in files_2_processed:
        current_time = int(round(time.time() * 1000))
	print current_time

        print file
        file_handle = open(file)
        file_str = file_handle.readlines()[0].lstrip("null")
        print file_str
	file_dict = json.loads(file_str)
        old_time = file_dict["timestamp"]
	print old_time

        diff_time = current_time - old_time
	print diff_time

	new_file_path = "/opt/mount5/new_data/" + file
	f_out = open(new_file_path, "w+")
	f = f_out.writelines([line.strip()+'\n' for line in parse_json_file(file, diff_time)])

# ==============================================================================
# Main
# ==============================================================================
if __name__ == "__main__":
    main()
















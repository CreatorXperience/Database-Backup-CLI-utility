#!.venv/bin/python3
import subprocess
import platform
from os import path, environ
import click
import functools
import tarfile

arch = ["x86_64", "arm", "x86"]
downloaded_path = path.expanduser("~/Downloads/gcloud.tar.gz")
gcloud_extract_dir = path.expanduser("~/Documents/")
shells = ["/bin/zsh", "/bin/bash"]

gcloud_source_map = {
    "x86_64": "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz",
    "arm": "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-arm.tar.gz",
    "x86": "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86.tar.gz",
}


def tar_filter(tar_info, tar_info2):
    print(tar_info, tar_info2)
    return tar_info


def install_google_cloud_on_os():
    proc = subprocess.Popen(
        ["bash", gcloud_extract_dir + "google-cloud-sdk/install.sh", "-q"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = proc.communicate()

    print(stdout)
    if stderr:
        print(f"Error occured while installing google-cloud {stderr}")
        proc.terminate()
        return

    subprocess.run(
        ["source", gcloud_extract_dir + "google-cloud-sdk/completion.zsh.inc"],
        check=True,
        capture_output=True,
    )
    user_shell = environ["SHELL"]
    if user_shell in shells:
        shell_idx = shells.index(user_shell)
        shell = shells.pop(shell_idx).strip("/bin/")

        subprocess.run(
            ["source", gcloud_extract_dir + f"google-cloud-sdk/{shell}"],
            check=True,
            capture_output=True,
        )
    print("installation complete")


def extract_gcloud(get_gcloudcli):
    @functools.wraps(get_gcloudcli)
    def get_gcloud_cli_wrapper(*args, **kwargs):
        get_gcloudcli(*args, **kwargs)

        if path.exists(downloaded_path):
            with tarfile.open(downloaded_path, "r:gz") as tar_f:
                tar_f.extractall(path.dirname(gcloud_extract_dir), filter=tar_filter)
                install_google_cloud_on_os()

            # print(tar_file)

    return get_gcloud_cli_wrapper


@extract_gcloud
def get_gcloud_on_os():
    try:
        subprocess.run(
            ["gcloud", "init"], capture_output=True, universal_newlines="\n", check=True
        )
    except FileNotFoundError as _:
        sys_arch = platform.machine()
        if sys_arch in gcloud_source_map.keys():
            try:
                subprocess.run(
                    [
                        "curl",
                        "-o",
                        downloaded_path,
                        gcloud_source_map[sys_arch],
                    ],
                    check=True,
                )
                print("suppported operating os")
            except KeyboardInterrupt as _ki:
                print("\n the installation was interrupted \n")


# get_gcloud_on_os()

install_google_cloud_on_os()
# install_gcloud_with_pexpect()


@click.command
def command():
    pass


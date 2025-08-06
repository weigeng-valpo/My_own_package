from setuptools import setup, find_packages

version = '2.11.6'
update_message = f'update pull util with snp_member v{version}'

print("                                                                            ") 
print("                                                                            ") 
print("                                                                            ") 
print("                                                                            ") 
print("                                                                            ") 
print("                                                                            ") 
print("############################################################################")         
print("#                                                                          #")
print("#                Are you tired to find the most recent DSNP queries?       #")
print("#                                                                          #")
print("#                Are you tired to copy and paste queries?                  #")
print("#                                                                          #")
print("#                Let's work together!                                      #")
print("#                                                                          #")
print("#                 **** DSNP QUERY BOX ****                                 #")
print(f"#                        Version {version}                                     #")
print("#                                                                          #")
print("############################################################################")


with open('requirements.txt') as f:
    required = f.read().splitlines()


setup(
    name = 'snp_query_box',
    version = version,
    description = 'keep dsnp related queries',
    author='Nathan Lim',
    author_email = 'LimS@aetna.com',
    packages = find_packages(),
    python_requires='>=3.6',
    install_requires = required,
    package_data = {'snp_query_box':['data/*.txt', 'data/*.csv', 'data/*.parquet']},
    include_package_data = True
)

print("                                 ") 
print("                                 ") 
print("*********************************")
print("                                 ")
print("  Installation complete!         ") 
print(f"      Version {version}          ")
print("*********************************")
print("                                 ") 
print("  try 'import snp_query_box'    ")
print("       in python                 ")
print("                                 ")
print("                                 ")



print("############################################################################") 
print(f"{update_message}")
print("############################################################################") 

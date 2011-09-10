"""
creates bootstrap script bigjob-bootstrap.py that installs bigjob Python package
""" 
import virtualenv, textwrap

def create_bigjob_bootstrap_script():
    output = virtualenv.create_bootstrap_script(textwrap.dedent("""
    import os, subprocess
    def after_install(options, home_dir):
        etc = join(home_dir, 'etc')
        if not os.path.exists(etc):
            os.makedirs(etc)         
        subprocess.call([join(home_dir, 'bin', 'easy_install'),
                     'bigjob'])    
    """))
    return output

if __name__ == "__main__" :
    output = create_bigjob_bootstrap_script()
    f = open('bigjob-bootstrap.py', 'w').write(output)
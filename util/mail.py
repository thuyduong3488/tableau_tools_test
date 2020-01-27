from config import CONFIG
import subprocess
import warnings


class AutoMail:
    """ A python singleton class that uses the automail.exe """
    AUTOMAIL_PATH = CONFIG['automail_path'][0]
    SUCCESS_RETURN_CODE = 0

    class __impl:
        """ Implementation of the singleton interface """

        def get_id(self):
            """ Test method, return singleton id """
            return id(self)

    # storage for the instance reference
    __instance = None

    def __init__(self):
        """ Create singleton instance """
        # Check whether we already have an instance
        if AutoMail.__instance is None:
            # Create and remember instance
            AutoMail.__instance = AutoMail.__impl()

        # Store instance reference as the only member in the handle
        self.__dict__['_Singleton__instance'] = AutoMail.__instance

    def __getattr__(self, attr):
        """ Delegate access to implementation """
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        """ Delegate access to implementation """
        return setattr(self.__instance, attr, value)

    def send(self, to=None, subject=None, message=None, attachment=None, text=None):
        """
        Sends email using the automail.exe
        :param to: str or list of str. Email address(es) of the receiver(s)
        :param subject: str
        :param message: str
        :param attachment: str
        :param text: str
        :return: bool
        """
        assert isinstance(to, str) or isinstance(to, list), 'The receiver email address must be a string or list of strings'
        if isinstance(to, str):
            to = [to]
        list_email_commands = [[self.AUTOMAIL_PATH, '-t', t] for t in to]
        if subject is not None:
            assert isinstance(subject, str) or isinstance(subject, list), 'The subject must be a string or list of strings'
            for command in list_email_commands:
                command.extend(('-s', subject))
        if message is not None:
            assert isinstance(message, str) or isinstance(message, list), 'The message must be a string or list of strings'
            for command in list_email_commands:
                command.extend(('-m', message))
        if attachment is not None:
            assert isinstance(attachment, str) or isinstance(attachment, list), 'The attachment path must be a string or list of strings'
            for command in list_email_commands:
                file_path = attachment
                command.extend(('-a', file_path))
        if text is not None:
            assert isinstance(text, str) or isinstance(text, list), 'The text path must be a string or list of strings'
            for command in list_email_commands:
                command.extend(('-mf', text))
        list_response_codes = []
        for command in list_email_commands:
            response = subprocess.run(command, stderr=subprocess.PIPE, shell=True)
            list_response_codes.append(response.returncode)
            if response.returncode != self.SUCCESS_RETURN_CODE:
                warnings.warn('AutoMail did not work for the receiver ' + str(command[2]))
        return all(rc == self.SUCCESS_RETURN_CODE for rc in list_response_codes)

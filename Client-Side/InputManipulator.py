class InputManipulator:

    def __init__(self, client_message):
        self.client_message = client_message.strip()
        self.validate_input()

    def validate_input(self):
        inputCommands = self.client_message.split(" ")
        if len(inputCommands) < 3:
            raise Exception("The command should contain at least three arguments")
        else:
            command_type = inputCommands[0].lower()
            valid_command_types = ["create", "drop", "insert", "delete", "select"]

            if command_type not in valid_command_types:
                raise Exception("Client-Side must enter the command type (create, drop, insert, delete, select)")
            else:
                self.validate_command_type(command_type, inputCommands)

    @staticmethod
    def validate_command_type(command_type, command_parts):

        if command_type == "create" or command_type == "drop":
            instance_type = command_parts[1].lower()
            valid_instance_types = ["database", "table", "index"]

            if instance_type not in valid_instance_types:
                raise Exception("Instance type for create/drop is required (database, table, index)")

        if command_type == "select" and "from" not in command_parts and "on" not in command_parts:
            raise Exception("Incomplete select command")

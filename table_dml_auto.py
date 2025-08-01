Option Explicit

Dim tbl, col
Dim targetTableCode
Dim found, updatedCount
Dim runUpdate

' Initialize
updatedCount = 0
found = False
runUpdate = True

' Prompt user for PHYSICAL table name (Code)
targetTableCode = InputBox("Enter the physical table name to update (e.g. CUSTOMER_TBL):", "Target Table")

If targetTableCode = "" Then
    MsgBox "No table name entered. Script cancelled.", vbExclamation, "Cancelled"
    runUpdate = False
End If

If runUpdate Then
    ' Search for the table using .Code (physical name)
    For Each tbl In ActiveModel.Tables
        If Not tbl.IsShortcut And UCase(tbl.Code) = UCase(targetTableCode) Then
            found = True
            For Each col In tbl.Columns
                If UCase(col.DataType) = "VARCHAR2" Then
                    ' Check for LengthSemantics property existence
                    On Error Resume Next
                    Dim colSemantics
                    colSemantics = col.LengthSemantics
                    If Err.Number <> 0 Then
                        ' If no LengthSemantics found, assume it's in BYTE, and update to CHAR
                        col.LengthSemantics = "CHAR"
                        updatedCount = updatedCount + 1
                        Err.Clear
                    Else
                        ' Update if semantics is not CHAR
                        If Trim(UCase(col.LengthSemantics)) <> "CHAR" Then
                            col.LengthSemantics = "CHAR"
                            updatedCount = updatedCount + 1
                        End If
                    End If
                    On Error GoTo 0
                End If
            Next
        End If
    Next

    If Not found Then
        MsgBox "Table with code '" & targetTableCode & "' not found in the model.", vbExclamation, "Table Not Found"
    ElseIf updatedCount = 0 Then
        MsgBox "No VARCHAR2 columns needed updating in table '" & targetTableCode & "'.", vbInformation, "No Changes Made"
    Else
        MsgBox updatedCount & " VARCHAR2 column(s) in table '" & targetTableCode & "' updated to use CHAR semantics.", vbInformation, "Update Complete"
    End If
End If
